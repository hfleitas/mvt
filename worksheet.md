# 🤿 Practice Worksheet 

## 🏅 Objectives
- Create a derived table from streaming data that's transformed by a kql function in real-time to persist the geofence output, customer code for arrivals and other columns.
- Automate actions based on insights (Bonus/Optional)

## 📃 Tasks
1. Create the target table using a limit 0 of the output of the function in .set control command. 
2. Enable the update policy for the new table.
3. Disable the update policy for the previous table.
4. Append from previous transformed table carrying over the extents creationTime.
5. Create backfilled materialized-view to get latest record by asset.

![image](https://github.com/user-attachments/assets/3f57f274-4184-4e42-9e9e-efe64b65edbc)

(Bonus/Optional)

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
- query the target table to reconcile using a count.
- See more hints in [syncagain.kql](kqlquerysets/skybitz/syncagain.kql) file.

### 5. Create View
Create a backfilled materialized-view for the current record.
```kql
.create materialized-view ...
{
<blank>
| summarize arg_max(<blank>,*) by <blank>
}
```

✅ Validations:
- leverage `.show operations | where operationid==guid(<blank>)` to monitor the progress of your async command.
- query the view to verify the count of rows and columns aligns as expected.
  

## 🏁 Finished
## 📖 Resources
- https://aka.ms/fabric-docs-rta
- https://aka.ms/adx.docs
- https://aka.ms/realtimeskill
###### ⚠️ Spoiler Alert ⚠️: [worksheet answers](kqlquerysets/wa.md)
