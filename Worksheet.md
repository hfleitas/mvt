# 🤿 Practice Worksheet 

## 🏅 Objectives
- Create a derived table with all data from stream transformed in real-time with geofence status and customer code.
- Automate actions based on insights

## 📃 Tasks
1. Create the target table using a limit 0 of the output of the function in .set control command. 
2. Enable the update policy
3. Append hist from raw prior to update policy enabling. 
4. Create backfilled materialized-view to get latest record by asset.

(Bonus/Optional)

5. Create a Real-Time Dashboard with a map and info.
6. Set an alert to automate an action, such as send a message when the truck/asset is running "late".

## 🪜 Steps 

**Hint:** Fill in the blanks.

### 1. Create Table
Using kql function `fn_sbt_TrailerLocationsGeofence` create an empty table.
```kql
.set-or-replace <blank> <| fn_sbt_TrailerLocationsGeofence | limit <blank>
```

Validations:
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

Validations:
- levergage `.show * policy update ` to check the status of your command.
- query the target table to get a count of rows being inserted by the update policy. 

### 3. Sync Hist
Backfill the new table with the hist from raw upto the point before you enabled the update policy.
```kql
.append
| <blank>
...
```

Validations:
- levergage `.show operations` to check the status of your command.
- query the target table to reconsile using a count. 

### 4. Create View
Create a backfilled materialized-view for the current record.
```kql
.create materialized-view ...
{
<blank>
| summarize arg_max(<blank>,*) by <blank>
}
```

Validations:
- leverage `.show operations | where operationid==guid(<blank>)` to monitor the progress of your async command.
- query the view to verify the count of rows and columns aligns as expected.
  

## 🏁 Finished
## 📖 Resources
- https://aka.ms/fabric-docs-rta
- https://aka.ms/adx.docs
- https://aka.ms/realtimeskill
