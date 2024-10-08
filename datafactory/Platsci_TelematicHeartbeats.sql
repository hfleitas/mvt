Select 
TelematicHeartbeats_ID,
Data_Id,
Unit_Number,
Unit_VIN,
Driver_Code,
Cvd_Id,
Tablet_Serial,
Event,
Logged_At_UTC,
Logged_At_MT,
Heartbeat_Id,
Speed,
Odometer,
Odometer_Jump,
Heading,
Ignition,
RPM,
Engine_Hours,
Engine_Hours_Jump,
Wheels_In_Motion,
Accuracy,
Satellites,
GPS_Valid,
HDOP,
Fuel_Level,
Total_Fuel_Used,
GPS_Latitude,
GPS_Longitude,
GPS_Description,
cast(GPS_GeoPoint as varchar(max)) as GPS_GeoPoint,
Message_Id,
Consumer_Version,
Origin_Version,
Timestamp,
Ignore_Data_Flag,
ETL_Load_Date_MT,
ETL_DW_PubRF_Load_Date,
GPS_State
from platsci.TelematicHeartbeats with(nolock)
