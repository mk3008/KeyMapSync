# KeyMapSync

KeyMapSync is micro ETL.

## Demo

## Feature
- Data sources can be defined in SQL
- Data source filtering
- Logging of executed SQL
- Transfer destination data can be reversed from the transfer source
- Multiple transfer destinations can be set for one data source
- Incremental transfer of data sources
- Data source offset transfer
- Automatic generation of control tables

## Constraints
- Must be a single DBMS (Transfers between DBMSs are not within the scope of the system)
- Supported DBMS is Postgres (There is a possibility to increase the supported DBMS in the future)
- Transfer speed depends on SQL
- A control table is automatically created in the DBMS

## Execution environment
.NET6

## Logic
- The data source to be transferred is output to a temporary table. This is called bridge data.
- Data sources are filtered to unlinked only. At this time, it is also possible to add any extraction conditions. (For example, transferring only yesterday's data, etc.)
- Bridge data is transferred to the destination.
- Bridge data is recorded so that it is not transferred twice. This recording table is called a keymap.
- Records whether the destination data was created with ETL. This record table is called Sync.
- Records whether the transfer destination data was transferred in which ETL process. This record table is called a Process.
- Records whether the destination data was transferred in which ETL transaction. This record table is called Transaction.
- By referring to the above keymap, Sync, Process, and Transaction tables, it is possible to look up the transfer source data from the transfer destination data.
