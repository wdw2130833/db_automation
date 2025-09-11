# Installation and Setup

## Prerequisites
1. **Database Requirement**: All SQL code must be executed on a **Microsoft SQL Server 2016 or higher**. Ensure the **CLR (Common Language Runtime)** feature is enabled on the SQL Server with **.NET Framework 4.7.2 or higher** installed.
2. **MySQL Support**: Install the **MySQL ODBC driver** on the same host as the MS SQL Server. Update the `odbc_driver` column in the `serverlist` table with the MySQL ODBC driver name.
   - Download: [MySQL ODBC Driver](https://dev.mysql.com/downloads/connector/odbc/)
3. **PostgreSQL Support**: Install the **PostgreSQL ODBC driver** on the same host as the MS SQL Server. Update the `odbc_driver` column in the `serverlist` table with the PostgreSQL ODBC driver name.
   - Download: [PostgreSQL ODBC Driver](https://www.postgresql.org/download/windows/)

## Installation Steps
1. **Open `installation.sql`**:
   - Use **SQL Server Management Studio (SSMS)** to connect to the SQL Server and open the `installation.sql` script.
   - Perform the following replacements in `installation.sql`:
     
     a. Replace `$(my_database)` with your target database name.
     
     b. Replace `$(temp_folder)` with the path to a local temporary folder accessible only by the MS SQL Server. The script will create this folder.
     
     c. If using a custom-built `db_automation.dll` (generated from `db_automation.sqlproj` in Visual Studio), replace `$(db_automation_dll)` with the full path to the `db_automation.dll` file.

     The script will automatically enable CLR if it is not already enabled.

2. **Configure Server List**:
   - Use the stored procedure `up_upsert_server` to initialize or manage servers in the `serverlist` table. Examples:
     ```sql
     -- Display all servers without changes
     EXEC up_upsert_server;

     -- Disable test server from installation.sql
     EXEC up_upsert_server 
         @servername = 'pgsql_test',
         @islive = 0;

     -- Delete test server from installation.sql
     EXEC up_upsert_server 
         @servername = 'Mysql_test',
         @delete = 1;

     -- Add a new MS SQL Server (uses Windows authentication by default)
     EXEC up_upsert_server 
         @servername = 'my_first_ms_sqlserver';

     -- Add a new MySQL Server to be managed by this automation platform:
     exec up_upsert_server 
       @servername= 'my_first_mysql'
	   ,@db_provider='MySQL'
	   ,@ip_or_dns='your_ip or dns name'
	   ,@port_number=3306
	   ,@username='mysql_username'
	   ,@pwd='pwd for mysql'
	   ,@odbc_driver='MySQL ODBC 9.4 Unicode Driver'  -- or other odbc driver name you installed
	   ,@connection_options='option=67108864;'   -- to support multiple SQL statements in a batch
      -- add a new PostgreSQL Server to be managed by this automation platform:
      exec up_upsert_server 
        @servername= 'my_first_pgsql'
   	   ,@db_provider='PostgreSQL'
   	   ,@ip_or_dns='your_ip or dns name'
   	   ,@port_number=5432
   	   ,@username='pgsql_username'
   	   ,@pwd='pwd for pgsql'
   	   ,@odbc_driver='PostgreSQL Unicode(x64)'  -- or other odbc driver name you installed
     
3. **Refresh Server List and DB list by SQL job or SQL query**:
   ```sql
	 -- Refresh serverlist for all live servers by this, or schedule this command as a SQL job:
     exec up_refresh_instances @remote_server='*'
     -- Refresh dblist for all databases on all live servers by this, or schedule this command as a SQL job:
     exec up_refresh_dblists @remote_server='*'
## Core stored procedures:
# up_call_sqlfunction Stored Procedure

## Overview

The `up_call_sqlfunction` stored procedure is a key component of the database automation platform, designed to execute SQL functions or queries across multiple SQL Server, MySQL, or PostgreSQL database instances in a distributed environment. It supports flexible targeting of servers, databases, instance types, and environments, with options for dry runs, debugging, logging, and error handling. The procedure integrates with external systems (e.g., Jira and Salesforce) for logging task outcomes and supports JSON-based input and output for configuration and results.

This procedure is intended for long-running database tasks (e.g., schema migrations, index creation, or data cleanup) across thousands of databases on hundreds of instances in multiple global regions. It provides a robust mechanism to execute SQL commands, manage parallelism, and track execution status.

## Prerequisites

- **Database**: SQL Server (MSSQL) with access to the `serverlist`, `dblist`, `instance_types`, `environments`, and `sql_functions` tables.
- **Permissions**: Execute permissions on the procedure and access to target databases.
- **Dependencies**: The `up_exec_remote_sql` stored procedure for remote execution.
- **Configuration**: Properly populated `serverlist` and `dblist` tables with server and database details (e.g., host, port, credentials, database provider).
- **JSON Support**: SQL Server 2016 or later for JSON parsing (`ISJSON`, `JSON_VALUE`, `OPENJSON`).

## Parameters

| Parameter | Type | Description | Default Value |
|-----------|------|-------------|---------------|
| `@The_SQL` | `nvarchar(max)` | The SQL function name (from `sql_functions` table) or raw SQL query to execute. Required. | `''` |
| `@json_input` | `nvarchar(max)` | JSON string containing configuration (e.g., server list, database list, arguments). Must be valid JSON. | `'{}'` |
| `@json_return` | `nvarchar(max) OUTPUT` | JSON output containing灾

System: The `[dbo].[up_call_sqlfunction]` stored procedure is a powerful tool for automating SQL execution across multiple database instances. Below is the continuation of the help documentation formatted for GitHub in Markdown, picking up from where the previous response was cut off due to the character limit. This completes the documentation with details on parameters, usage examples, return codes, and additional notes.

```markdown
| `@json_return` | `nvarchar(max) OUTPUT` | JSON output containing execution results, including status, server, database, affected rows, and errors. | `'{}'` |
| `@error_msg` | `nvarchar(max) OUTPUT` | Error message if the procedure fails. | `''` |
| `@servername_or_list` | `varchar(max)` | Comma-separated list of server names or `'*'` for all servers. | `''` |
| `@dbname_or_list` | `nvarchar(max)` | Comma-separated list of database names, `'*'` for all databases, or `'default'` for default databases (e.g., `master` for MSSQL, `postgres` for PostgreSQL). | `''` |
| `@return_type` | `nvarchar(100)` | Return format for results (`'json'` or empty for temp table). | `''` |
| `@return_temp_table` | `varchar(128)` | Name of the temporary table to store results (e.g., `#tmp_default`). | `''` |
| `@instance_types` | `varchar(4000)` | Comma-separated list of instance types or `'*'` for all. | `'*'` |
| `@db_types` | `varchar(4000)` | Comma-separated list of database types (`MSSQL`, `MySQL`, `PostgreSQL`) or `'*'` for all. | `'*'` |
| `@environment_or_list` | `nvarchar(max)` | Comma-separated list of environments or `'*'` for all. | `'*'` |
| `@cmd_type` | `tinyint` | Command type (0 for SQL execution, others reserved for future use). | `0` |
| `@dry_run` | `bit` | If `1`, simulate execution without running SQL. | `0` |
| `@debug` | `bit` | If `1`, output debug information from `#remote_exec_content`. | `0` |
| `@max_threads` | `int` | Maximum number of parallel threads for execution. Overrides server defaults. | `0` (uses server default, typically 5) |
| `@timeout` | `int` | Timeout in seconds for each SQL execution. | `60` |
| `@include_remote_info` | `bit` | If `1`, add server and database names to the return temp table. | `0` |
| `@log` | `bit` | If `1`, log execution details to `remote_sql_log` table. | `1` |
| `@stop_by_error` | `bit` | If `1`, stop execution on error. | `1` |
| `@raise_error` | `bit` | If `1`, raise errors using `THROW`. | `0` |

## Usage

### Syntax
```sql
EXEC [dbo].[up_call_sqlfunction]
    @The_SQL,
    @json_input = '{}',
    @json_return = '{}' OUTPUT,
    @error_msg = '' OUTPUT
    
1. **Call REST API**:
   ```sql
    declare @api_return nvarchar(max),@api_uri varchar(4000)
	set @api_uri='https://data.nasdaq.com/api/v3/datatables/NDW/EQTA?date=2025-08-29&symbol=GOOGL-US&api_key=JwPfRGfkZRN48zKfDTvL'
	exec [up_call_rest_api] @api_url=@api_uri,@content='',@api_return=@api_return output
	select @api_return
       
