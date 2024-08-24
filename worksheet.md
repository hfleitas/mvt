# 🤿 Practice Worksheet 
## Pre-reqs
- Must have at least contributor access to the Fabric workspace where the data resides and a Fabric license (ie. Trial/etc). 
- Read the [Fabric Eventhouse Overview](https://learn.microsoft.com/fabric/real-time-intelligence/eventhouse).
- Read the [Create KQL Queryset](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/create-query-set) guide.

## 🏅 Objectives
- Create a derived table from streaming data that's transformed by a kql function in real-time to persist the geofence output, customer code for arrivals and other columns.
- Automate actions based on insights (Bonus/Optional)

## 📃 Tasks
1. Create the target table using a limit 0 of the output of the function in .set control command. 
2. Enable the update policy for the new table.
3. Disable the update policy for the previous table.
4. Append from previous transformed table carrying over the extents creationTime.
5. Create backfilled materialized-view to get latest record by asset.

![image](https://github.com/user-attachments/assets/3f57f274-4184-4e42-9e9e-efe64b65edbc "Steps Diagram")

### Legend
- 🟦 blue means existing items.
- 🟩 green are new items you'll create while completing this worksheet.

### Bonus/Optional

6. Create a Real-Time Dashboard with a map and info.
7. Set an alert to automate an action, such as send a message when the truck/asset is running "late".

## 🪜 Steps 

**Hint:** Fill in the blanks.

### 1. Create Table
Using kql function `fn_sbt_TrailerLocationsGeofence` create an empty table.
```kql
.set-or-replace <blank> <| fn_sbt_TrailerLocationsGeofence | limit <blank>
```

✅ Validations:
- leverage `.show tables` to confirm the new table exists.
- query the table to make sure it exists and that it's empty with the expected schema columns.


### 2. Update Policy
Enable the update policy so as data lands on the raw table (source table) it gets transformed automatically to the new table (derrived/target table).
````kql
.alter table <blank> policy update
```
[
    {
        "IsEnabled": true,
        "Source": "<blank>",
        "Query": "<blank>"
    }
]
```
````

✅ Validations:
- levergage `.show table * policy update ` to check the status of your command.
- query the target table to get a count of rows being inserted by the update policy. 


### 3. Update Policy - disable
Disable the update policy policy on the previous transformed table to avoid duplicates on the next step. 


### 4. Sync Hist
Backfill the new table with the hist from raw upto the point before you enabled the update policy.
```kql
.set-or-append <blank> with(<blank>='<blank>') <|
<blank>
...
```

👀 Hint
```kql
// get a large extent to test
.show table skybitz_TrailerLocations extents | sort by MaxCreatedOn asc
| project ExtentId, RowCount, MaxCreatedOn
| where RowCount > 1000000
| top 1 by MaxCreatedOn asc

// add geofence...
.set-or-append async TrailerLocationsTest with(creationTime='2021-09-30 23:59:59.9990') <|
let sbt=materialize(skybitz_TrailerLocations
| where extent_id() == guid(9142fc9c-d9d3-4596-92aa-2bda40839e64)
| extend covering = geo_point_to_s2cell(sbt_longitude, sbt_latitude, 8));
let polygons = materialize (CustomerPolygons | project CustomerCode, Polygon, covering);
let arrival=sbt
| lookup (polygons | mv-expand covering to typeof(string)) on covering
| project-away covering
| where geo_point_in_polygon(sbt_longitude, sbt_latitude, Polygon)
| summarize CustomerCode=make_list(CustomerCode) by sbt_assetid, sbt_messagetimestampmst,sbt_mtsn,sbt_messagetype,sbt_tetherstate,sbt_tirestate,sbt_extpwr,sbt_movementstate,sbt_cargostate,sbt_latitude,sbt_longitude,sbt_battery,sbt_quality,sbt_geoname,sbt_city,sbt_state,sbt_country,sbt_geotypename,sbt_idlestatus,sbt_idleduration,sbt_idlegap,sbt_skyfencestatus,sbt_speed,sbt_heading,sbt_transid,etl_timestampUTC,CurrentRow //,geofence
| distinct sbt_assetid, sbt_messagetimestampmst,sbt_mtsn,sbt_messagetype,sbt_tetherstate,sbt_tirestate,sbt_extpwr,sbt_movementstate,sbt_cargostate,sbt_latitude,sbt_longitude,sbt_battery,sbt_quality,sbt_geoname,sbt_city,sbt_state,sbt_country,sbt_geotypename,sbt_idlestatus,sbt_idleduration,sbt_idlegap,sbt_skyfencestatus,sbt_speed,sbt_heading,sbt_transid,etl_timestampUTC,CurrentRow, tostring(CustomerCode) //,geofence
| extend geofence='arrival';
let departure=sbt
| project-away covering
| join kind=leftanti arrival on sbt_assetid,sbt_messagetimestampmst, sbt_mtsn,sbt_messagetype,sbt_tetherstate,sbt_tirestate,sbt_extpwr,sbt_movementstate,sbt_cargostate,sbt_latitude,sbt_longitude,sbt_battery,sbt_quality,sbt_geoname,sbt_city,sbt_state,sbt_country,sbt_geotypename,sbt_idlestatus,sbt_idleduration,sbt_idlegap,sbt_skyfencestatus,sbt_speed,sbt_heading,sbt_transid,etl_timestampUTC,CurrentRow
| extend geofence='departed';
union arrival, departure
```

✅ Validations:
- levergage `.show operations` to check the status of your command.
- use `.show table <blank> extents` & `strcat()` to automate the async commands for all extents. ie. [scriptedappend.kql](kqlquerysets/skybitz/scriptedappend.kql)
- use `.show table <blank> extents` for both tables to reconcile `RowCount` per extent & matching `MaxCreatedOn`.
- query the target table to reconcile using a count.
- See more hints in [syncagain.kql](kqlquerysets/skybitz/syncagain.kql) file.
- See [Ingestion properties](https://learn.microsoft.com/kusto/ingestion-properties)

### 5. Create View
Create a backfilled materialized-view for the current record.
```kql
.create materialized-view with (<blank>=<blank>, <blank>='<blank>') <blank> on <blank>
{
<blank>
| summarize arg_max(<blank>,*) by <blank>
}
```

👀 Hint
```
// lookup the documentation for kql materialized-views. 

// look at the Query
.show materialized-views
```

✅ Validations:
- see syntax [example](https://github.com/hfleitas/mvt/blob/5cce0ed86feab91c1913b8e4a28432978e69cb1b/kqlquerysets/geofence/optimize.kql#L17) for backfill & more. 
- leverage `.show operations | where operationid==guid(<blank>)` to monitor the progress of your async command.
- query the view to verify the count of rows and columns aligns as expected.
  

## 🏁 Finished
## 📖 Resources
- https://aka.ms/fabric-docs-rta
- https://aka.ms/adx.docs
- https://aka.ms/realtimeskill

## Bonus/Optional
6. Create a Real-Time Dashboard with a map and info.
```kql
<blank>
...
| project label=strcat(sbt_assetid,': ',geofence,' at ', tostring(sbt_messagetimestampmst), CustomerCode), sbt_assetid, geofence, CustomerCode, sbt_longitude=toreal(sbt_longitude), sbt_latitude=toreal(sbt_latitude)
| where (isnotempty(sbt_longitude) or isnotempty(sbt_latitude)) //and sbt_assetid =='5310' 
| render scatterchart with (kind=map)
```

7. Set an alert to automate an action, such as send a message when the truck/asset is running "late".
