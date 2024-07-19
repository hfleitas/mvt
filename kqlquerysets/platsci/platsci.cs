using System;
using System.Linq;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Newtonsoft.Json;
using PlatformScience.Integrations.Core;
using PlatformScience.Integrations.Models;
using PlatformScience.Integrations.Models.Helpers;
using PlatformScience.Integrations.Models.Payloads;
using PlatformScience.Integrations.WebJob.Core.DbContexts.DW_PubRF;

namespace PlatformScience.Integrations.TelematicHeartbeatRelay
{
    public static class TelematicHeartbeatParser
    {
        public static TelematicHeartbeats ParsePayload(string payload)
        {
            var integrationMessage = JsonConvert.DeserializeObject<WorkflowIntegrationMessage>(payload);
            if(integrationMessage?.MessageQueueType.ToLowerInvariant().Trim() == "heartbeat")
            {
                return null;
            }

            var message    = JsonConvert.DeserializeObject<AmqpPayload<dynamic>>(integrationMessage.FullMessage);
            var powerUnit  = message.Data.Relationships.Assets.Data.FirstOrDefault(d => d.Type == "power_unit");
            var user       = message.Data.Relationships.Users.Data.FirstOrDefault(d => d.Type == "user");
            var driverCode = user?.Attributes.ExternalId;
            _driverCodesWhitelist.UpdateWhitelist();
            if(_driverCodesWhitelist.IsWhitelisted(driverCode))
            {
                return null;
            }

            var cvd                 = message.Data.Relationships.Devices.Data.FirstOrDefault(d => d.Type == "cvd");
            var tablet              = message.Data.Relationships.Devices.Data.FirstOrDefault(d => d.Type == "tablet");
            var attributes          = message.Data.Attributes;
            var locationDescription = DynamicsHelper.DynamicGetOrDefault<string>(() => attributes.location.description);
            var stateString         = locationDescription?.Split(' ', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
            var locationState       = stateString?.Substring(Math.Max(0, stateString.Length - 5));
            var loggedAt            = DynamicsHelper.DynamicGetOrDefault<DateTime>(() => attributes.logged_at);
            var latitude            = DynamicsHelper.DynamicGetOrDefault<decimal?>(() => attributes.location.latitude);
            var longitude           = DynamicsHelper.DynamicGetOrDefault<decimal?>(() => attributes.location.longitude);
            var odometer            = DynamicsHelper.DynamicGetOrDefault<decimal?>(() => attributes.odometer);

            Geometry geoPoint = null;
            if(latitude != null && longitude != null)
            {
                geoPoint = GeometryFactory.CreatePoint(new Coordinate((double) longitude.Value, (double) latitude.Value));
            }

            return new TelematicHeartbeats
            {
                DataId       = message.Data.Id,
                UnitNumber   = powerUnit?.Attributes.ExternalId,
                UnitVin      = powerUnit?.Attributes.HardwareId,
                DriverCode   = driverCode,
                CvdId        = cvd?.Id,
                TabletSerial = tablet?.Attributes?.Serial,

                Event           = DynamicsHelper.DynamicGetOrDefault(() => attributes.@event),
                LoggedAtUtc     = loggedAt,
                LoggedAtMt      = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(loggedAt, "Mountain Standard Time"),
                HeartbeatId     = DynamicsHelper.DynamicGetOrDefault(() => attributes.heartbeat_id),
                Speed           = DynamicsHelper.DynamicGetOrDefault(() => attributes.speed),
                Odometer        = odometer,
                OdometerJump    = DynamicsHelper.DynamicGetOrDefault(() => attributes.odometer_jump),
                Heading         = DynamicsHelper.DynamicGetOrDefault(() => attributes.heading),
                Ignition        = DynamicsHelper.DynamicGetOrDefault(() => attributes.ignition),
                Rpm             = DynamicsHelper.DynamicGetOrDefault(() => attributes.rpm),
                EngineHours     = DynamicsHelper.DynamicGetOrDefault(() => attributes.engine_hours),
                EngineHoursJump = DynamicsHelper.DynamicGetOrDefault(() => attributes.engine_hours_jump),
                WheelsInMotion  = DynamicsHelper.DynamicGetOrDefault(() => attributes.wheels_in_motion),
                Accuracy        = DynamicsHelper.DynamicGetOrDefault(() => attributes.accuracy),
                Satellites      = DynamicsHelper.DynamicGetOrDefault(() => attributes.satellites),
                GpsValid        = DynamicsHelper.DynamicGetOrDefault(() => attributes.gps_valid),
                Hdop            = DynamicsHelper.DynamicGetOrDefault(() => attributes.hdop),
                FuelLevel       = DynamicsHelper.DynamicGetOrDefault(() => attributes.fuel_level),
                TotalFuelUsed   = DynamicsHelper.DynamicGetOrDefault(() => attributes.total_fuel_used),
                GpsLatitude     = latitude,
                GpsLongitude    = longitude,
                GpsDescription  = locationDescription,
                GpsGeoPoint     = geoPoint,
                GpsState        = locationState,

                MessageId       = message.Meta.MessageId,
                ConsumerVersion = message.Meta.ConsumerVersion,
                OriginVersion   = message.Meta.OriginVersion,
                Timestamp       = message.Meta.Timestamp.DateTime,
                IgnoreDataFlag  = !(odometer > 0),

                EtlLoadDateMt   = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(DateTime.UtcNow, "Mountain Standard Time")
            };
        }

        private static readonly GeometryFactory      GeometryFactory       = NtsGeometryServices.Instance.CreateGeometryFactory(4326);
        private static readonly DriverCodesWhitelist _driverCodesWhitelist = new DriverCodesWhitelist();
    }
}
