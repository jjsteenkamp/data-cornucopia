# JDBC Schema Incrementer

The aim of the JDBC Schema Incrementer is to provide the ability to update a database table schema safely as part of a deployment cycle whilst retaining the data.

It was developed with a specific class of 'databases' in mind where only a subset of the SQL functionality is available. E.g. Apache Impala. Also, another key design consideration is that it should work off the base 'CREATE TABLE' statements rather than requiring developers to do a seperate 'ALTER TABLE' statement 

The aim for this incrementer is to achieve the following:

* Detect whether a table schema migration is required - i.e. is this a new table.
* Detection from the table creation scripts whether we add new columns, rename columns, column type changes, and column removals.
* Generation of scripts for inspection (e.g. dry-run mode) or execution
* Some basic rollback utilities.








    