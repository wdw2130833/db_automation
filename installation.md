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
| `@json_return` | `nvarchar(max) OUTPUT` | JSON output containing execution results, including status, server, database, affected rows, and errors. | `'{}'` |
| `@error_msg` | `nvarchar(max) OUTPUT` | Error message if the procedure fails. | `''` |

### Key parameters support in `@json_input` 

| Key name  | Type | Description | Default Value |
|-----------|------|-------------|---------------|
| `servername_or_list` | `varchar(max)` | Comma-separated list of server names or `'*'` for all servers. | `''` |
| `dbname_or_list` | `nvarchar(max)` | Comma-separated list of database names, `'*'` for all databases, or `'default'` for default databases (e.g., `master` for MSSQL, `postgres` for PostgreSQL). | `''` |
| `return_type` | `nvarchar(100)` | Return format for results (`'json'` or empty for temp table). | `''` |
| `return_temp_table` | `varchar(128)` | Name of the temporary table to store return results (e.g., `#tmp_default`). create this temp table before call up_call_sqlfunction: <br> if object_id(''tempdb..#tmp_result'') is not null <br> drop table #tmp_result <br> create table #tmp_result (run_id uniqueidentifier) | `''` |
| `instance_types` | `varchar(4000)` | Comma-separated list of instance types or `'*'` for all. | `'*'` |
| `db_types` | `varchar(4000)` | Comma-separated list of database types (`MSSQL`, `MySQL`, `PostgreSQL`) or `'*'` for all. | `'*'` |
| `environment_or_list` | `nvarchar(max)` | Comma-separated list of environments or `'*'` for all. | `'*'` |
| `cmd_type` | `tinyint` | Command type (0 for SQL execution, 1 for sqlcmd, mysql or psql, 2 for bcp, mysqldump or pg_dump, others reserved for future use). | `0` |
| `dry_run` | `bit` | If `1`, simulate execution without running SQL. | `0` |
| `debug` | `bit` | If `1`, output debug information from `#remote_exec_content`. | `0` |
| `max_threads` | `int` | Maximum number of parallel threads for execution. Overrides server defaults. | `0` (uses server default, typically 5) |
| `timeout` | `int` | Timeout in seconds for each SQL execution. | `60` |
| `include_remote_info` | `bit` | If `1`, add server and database names to the return temp table. | `0` |
| `log` | `bit` | If `1`, log execution details to `remote_sql_log` table. | `1` |
| `stop_by_error` | `bit` | If `1`, stop execution on error, ignore the rest of SQLs, only applied for MS SQL. | `1` |
| `raise_error` | `bit` | If `1`, raise errors using `THROW`. | `0` |

## Usage
Please check the detail usage in up_refresh_instances or up_refresh_dblists.
### Examples:
```sql
declare @json_input nvarchar(max)=N'{}',@json_return nvarchar(max)=N'{}', @the_sql nvarchar(max), @error_msg nvarchar(max)=N''
if object_id('tempdb..#tmp_result') is not null
     drop table #tmp_result
create table #tmp_result (run_id uniqueidentifier)
set @the_sql=N'select name from sys.databases'
set @json_input=JSON_MODIFY(@json_input,'$.servername_or_list','*')
set @json_input=JSON_MODIFY(@json_input,'$.db_types','MSSQL')
set @json_input=JSON_MODIFY(@json_input,'$.dbname_or_list','default')
set @json_input=JSON_MODIFY(@json_input,'$.return_temp_table','#tmp_result')
set @json_input=JSON_MODIFY(@json_input,'$.include_remote_info',1)

EXEC [dbo].[up_call_sqlfunction]
    @The_SQL=@the_sql,
    @json_input = @json_input,
    @json_return =@json_return out,
    @error_msg =@error_msg out
--- access the result       
select * from #tmp_result
```

# up_call_os_cmd Stored Procedure

## Overview

The `up_call_os_cmd` stored procedure executes an operating system command on local host, with configurable options provided via a JSON input. It supports logging, debugging, dry runs, and error handling, with results optionally stored in a temporary table.

## Parameters
| Parameter | Type | Description | Default Value |
|-----------|------|-------------|---------------|
| `@cmd` | `nvarchar(4000)` | The command application to execute. This command must be searchable in path if not provide full path to the command. | `not empty` |
| `@json_input` | `nvarchar(max)` | JSON string containing optional configuration settings (e.g., arguments, dry_run, debug, log, return_temp_table, raise_error). Must be valid JSON. | `'{}'` |
| `@cmd_return` | `nvarchar(max)` | Output parameter returning the result of the executed command. | `'{}'` |
| `@error_msg` | `nvarchar(max)` | Output parameter capturing any error messages generated during execution. | `''` |

### JSON Input Fields (in `@json_input`)
| Field | Type | Description | Default Value |
|-------|------|-------------|---------------|
| `arguments` | `nvarchar(max)` | Additional arguments for the command.| `''` |
| `return_temp_table` | `varchar(max)` | Name of the temporary table to store results. Must exist in `tempdb` and be created before if specified. | `''` |
| `dry_run` | `bit` | If `1`, simulates execution and returns configuration without running the command. | `0` |
| `debug` | `bit` | If `1`, outputs detailed execution information from `#remote_exec_content`. | `0` |
| `log` | `bit` | If `1`, logs execution details to `remote_sql_log` table. | `1` |
| `raise_error` | `bit` | If `1`, raises an error on failure instead of returning it in `@error_msg`. | `0` |

## Usage
The procedure validates inputs, executes the command using `up_exec_remote_sql`, and handles results or errors based on configuration. It supports:
- **Dry Run**: Preview execution details without running the command.
- **Logging**: Stores execution details in `remote_sql_log`.
- **Temporary Table Output**: Stores results in a specified temporary table.
- **Error Handling**: Returns errors via `@error_msg` or raises them if `raise_error=1`.

## Return Values
- **0**: Success.
- **-100**: Invalid JSON input or non-existent temporary table.
- **-102**: Execution error (details in `@error_msg`).
- **-110**: Empty `@cmd` parameter.

## Error Handling
- Validates `@cmd` for non-empty input.
- Checks `@json_input` for valid JSON format.
- Ensures specified temporary table exists (if provided).
- Captures errors in `@error_msg` or raises them if `raise_error=1`.

## Notes
- The procedure uses a temporary table `#remote_exec_content` to store execution metadata.
- The `up_exec_remote_sql` procedure is called internally to execute the command.
- Sensitive data (e.g., passwords) is masked in `#remote_exec_content` before logging.
- Ensure the `remote_sql_log` table exists if `@log=1`.

### Examples:
```sql
declare @return int, @json_input nvarchar(max)=N'{}',@cmd_return nvarchar(max)=N'{}', @cmd nvarchar(max), @error_msg nvarchar(max)=N''
if object_id('tempdb..#tmp_result') is not null
     drop table #tmp_result
create table #tmp_result (run_id uniqueidentifier)
set @cmd=N'ipconfig '
set @json_input=JSON_MODIFY(@json_input,'$.return_temp_table','#tmp_result')
set @json_input=JSON_MODIFY(@json_input,'$.arguments','/all')
set @json_input=JSON_MODIFY(@json_input,'$.debug',0)

EXEC @return=[dbo].[up_call_os_cmd]
    @cmd=@cmd,
    @json_input = @json_input,
    @cmd_return =@cmd_return out,
    @error_msg =@error_msg out
if @return<>0
   select @return as return_error
--- access the result       
select * from #tmp_result
```

# up_call_rest_api Stored Procedure

## Overview
The `up_call_rest_api` stored procedure is designed to facilitate HTTP requests to RESTful APIs from within a SQL Server environment. It allows users to send HTTP requests (e.g., GET, POST) to a specified API endpoint, customize headers, content, and other request parameters, and retrieve the API response. The procedure is implemented as an external CLR (Common Language Runtime) stored procedure, leveraging the `db_automation.StoredProcedures.call_rest_api` assembly.

## Prerequisites
- **SQL Server**: The database server must have CLR integration enabled, as this stored procedure relies on an external CLR assembly.
- **Permissions**: The caller must have appropriate permissions to execute CLR stored procedures (`EXECUTE` permission on `up_call_rest_api`).
- **Network Access**: The SQL Server instance must have network access to the target API endpoint specified in `@api_url`.
- **CLR Assembly**: The `db_automation` assembly containing the `StoredProcedures.call_rest_api` class must be deployed in the SQL Server database.
- **SQL Server Version**: Compatibility with the SQL Server version where CLR integration is supported (e.g., SQL Server 2012 or later).

## Parameter Table
| Parameter       | Type             | Description                                                                 | Default Value |
|-----------------|------------------|-----------------------------------------------------------------------------|---------------|
| `@api_url`      | `nvarchar(4000)` | The URL of the REST API endpoint to call.                                   | None          |
| `@content`      | `nvarchar(max)`  | The request body content, typically used for POST or PUT requests.          | None          |
| `@headers`      | `nvarchar(4000)` | Custom HTTP headers in a string format (e.g., `'Content-Type: application/json'`). | `N''`         |
| `@method`       | `nvarchar(4000)` | The HTTP method for the request (e.g., `GET`, `POST`, `PUT`, `DELETE`).     | `N'GET'`      |
| `@content_type` | `nvarchar(4000)` | The content type of the request body (e.g., `application/json`).            | `N''`         |
| `@encode`       | `nvarchar(4000)` | The encoding type for the request content (if applicable).                  | `N''`         |
| `@accept`       | `nvarchar(4000)` | The expected response content type (e.g., `application/json`).              | `N''`         |
| `@useragent`    | `nvarchar(4000)` | The user-agent string for the HTTP request.                                 | `N''`         |
| `@api_return`   | `nvarchar(max)`  | Output parameter that captures the response from the API call.              | None          |

## Usage
Call any REST API with standard parameters.
### Examples:
```sql
declare @api_return nvarchar(max),@api_uri varchar(4000)
set @api_uri='https://data.nasdaq.com/api/v3/datatables/NDW/EQTA?date=2025-08-29&symbol=GOOGL-US&api_key=JwPfRGfkZRN48zKfDTvL'
exec [up_call_rest_api] @api_url=@api_uri,@content='',@api_return=@api_return output
select @api_return
```
