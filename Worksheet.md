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
6. Set an alert to automate an action.

## 🪜 Steps 

**Hint:** Fill in the blanks.
1. Using kql function `fn_sbt_TrailerLocationsGeofence` create an empty table.
```kql
.set-or-replace <blank> <| fn_sbt_TrailerLocationsGeofence | limit <blank>
```

2. Enable the update policy so as data lands on the raw table it gets transformed automatically to the new table.
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

3. Backfill the new table with the hist from raw upto the point before you enabled the update policy.
```kql
.append
| <blank>
...
```

4. Create a backfilled materialized-view for the current record.
```kql
.create materialized-view ...
{
<blank>
| summarize arg_max(<blank>,*) by <blank>
}

.show operations | where operationid==guid(<blank>)
```

## 🏁 Finished
## 📖 Resources
- https://aka.ms/fabric-docs-rta
- https://aka.ms/adx.docs
- https://aka.ms/realtimeskill
