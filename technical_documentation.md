# Database Automation Platform Documentation

## Table of Contents

- [Fundamental Core Stored Procedures](#fundamental-core-stored-procedures)
    - [up_call_sqlfunction](#up_call_sqlfunction)
    - [up_call_os_cmd](#up_call_os_cmd)
    - [up_execute_powershell](#up_execute_powershell)
    - [up_execute_aws_cli](#up_execute_aws_cli)
    - [up_call_rest_api](#up_call_rest_api)
- [Stored Procedures for MS SQLServer](#Stored-Procedures-for-MS-SQLServer)
    - [up_synch_backup_history](#up_synch_backup_history)
    - [up_WhoIsActive_alert](#up_WhoIsActive_alert)
    - [up_run_as_job](#up_run_as_job)
    - [up_asynch_task_refresh](#up_asynch_task_refresh)
## Fundamentals

# up_call_sqlfunction

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

### JSON Input Fields (in @json_input) 

| Key name  | Type | Description | Default Value |
|-----------|------|-------------|---------------|
| `servername_or_list` | `varchar(max)` | Comma-separated list of server names or `'*'` for all servers. | `''` |
| `dbname_or_list` | `nvarchar(max)` | Comma-separated list of database names, `'*'` for all databases, or `'default'` for default databases (e.g., `master` for MSSQL, `postgres` for PostgreSQL). | `''` |
| `return_type` | `nvarchar(100)` | Return format for results (`'json'` or empty for temp table). | `''` |
| `return_temp_table` | `varchar(128)` | Name of the temporary table to store return results (e.g., `#tmp_default`). create this temp table before call up_call_sqlfunction: <br> if object_id(''tempdb..#tmp_result'') is not null <br> drop table #tmp_result <br> create table #tmp_result (run_id uniqueidentifier) <br> | `''` |
| `instance_types` | `varchar(4000)` | Comma-separated list of instance types or `'*'` for all. | `'*'` |
| `db_types` | `varchar(4000)` | Comma-separated list of database types (`MSSQL`, `MySQL`, `PostgreSQL`) or `'*'` for all. | `'*'` |
| `environment_or_list` | `nvarchar(max)` | Comma-separated list of environments or `'*'` for all. | `'*'` |
| `cmd_type` | `varchar(20)` | Command type ( empty string for SQL execution, SQL_CMD for sqlcmd, mysql or psql, SQL_DUMP for bcp, mysqldump or pg_dump, OS_CMD for operation command application. Others reserved for future use). | `` |
| `include_ag_node` | `bit` | Only apply for MSSQL. If `1`, including all ag nodes . | `0` |
| `exclude_ag_listener` | `bit` | Only apply for MSSQL. If `1`, exclude ag listener. | `0` |
| `sql_parameters` | `nvarchar(max)` | json key values for sql variables in query. | `` |
| `arguments` | `nvarchar(max)` | extra aruments for SQL_CMD cmd type. | `` |
| `dry_run` | `bit` | If `1`, simulate execution without running SQL. | `0` |
| `debug` | `bit` | If `1`, output debug information from `#remote_exec_content`. | `0` |
| `max_threads` | `int` | Maximum number of parallel threads for execution. Overrides server defaults. | `0` (uses server default, typically 5) |
| `timeout` | `int` | Timeout in seconds for each SQL execution. | `60` |
| `include_remote_info` | `bit` | If `1`, add server and database names to the return temp table. | `0` |
| `log` | `bit` | If `1`, log execution details to `remote_sql_log` table. | `1` |
| `stop_by_error` | `bit` | If `1`, stop execution on error, ignore the rest of SQLs, only applied for MS SQL. | `1` |
| `raise_error` | `bit` | If `1`, raise errors using `THROW`. | `0` |

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

# up_call_os_cmd

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
# up_execute_powershell 

## Overview
The `up_execute_powershell` stored procedure executes PowerShell scripts by invoking the `powershell.exe` command through the `[dbo].[up_call_os_cmd]` procedure. It handles script execution, error handling, and returns results in a JSON format or a temporary table.

## Parameters

| Parameter           | Type          | Description                                                                 | Default Value       |
|---------------------|---------------|-----------------------------------------------------------------------------|---------------------|
| `@ps_scripts`       | `nvarchar(max)` | The PowerShell script to execute. Must not be null or empty.                | None                |
| `@return_temp_table`| `varchar(128)`  | Name of the temporary table to store results (if applicable).               | `#tmp_result`       |
| `@error_msg`        | `nvarchar(max)` | Output parameter capturing any error messages during execution.             | empty string |
| `@ps_result`        | `nvarchar(max)` | Output parameter containing the result of the PowerShell execution in JSON. | empty JSON |

## Return Value
- Returns an integer (`@return`) from the `[dbo].[up_call_os_cmd]` procedure, indicating the success or failure of the command execution.
- Returns `-100` if `@ps_scripts` is null or empty, with an error message set in `@error_msg`.

## Description
1. **Input Validation**: Checks if `@ps_scripts` is null or empty. If so, sets `@error_msg` to `'@ps_scripts is null or empty!'` and returns `-100`.
2. **Script Preparation**: Wraps the provided `@ps_scripts` in a PowerShell `try-catch` block to handle exceptions. Escapes double quotes in the script for proper execution.
3. **Command Construction**: Builds the `powershell.exe` command with the prepared script as an argument.
4. **JSON Input**: Constructs a JSON object containing the `@return_temp_table` and `@arguments` for passing to `up_call_os_cmd`.
5. **Execution**: Calls `[dbo].[up_call_os_cmd]` to execute the PowerShell command, capturing the result in `@ps_result` and any errors in `@error_msg`.
6. **Output**: Returns the result code from `up_call_os_cmd`.

## Example
```sql
DECLARE @error_msg nvarchar(max) = '';
DECLARE @ps_result nvarchar(max) = '{}';
DECLARE @return int;

if object_id('tempdb..#tmp_result') is not null
     drop table #tmp_result
create table #tmp_result (run_id uniqueidentifier)

EXEC @return = [dbo].[up_execute_powershell]
    @ps_scripts = 'Write-Output "Hello, World!"',
    @error_msg = @error_msg OUTPUT,
    @ps_result = @ps_result OUTPUT;
--- access the result       
select * from #tmp_result
```

## Notes
- The procedure assumes `up_call_os_cmd` is available and correctly configured to execute OS commands.
- The `@ps_scripts` parameter must contain valid PowerShell script content.
- The procedure escapes double quotes in the script to ensure proper execution.
- Errors from PowerShell execution are captured in the `@error_msg` output parameter.
- The `@ps_result` output is in JSON format, as determined by `up_call_os_cmd`.

# up_execute_aws_cli 

The `up_execute_aws_cli` stored procedure executes AWS CLI commands from within SQL Server, allowing interaction with AWS services. It constructs the command, processes parameters, and calls the underlying `[dbo].[up_call_os_cmd]` procedure to execute the command and handle results.

## Parameters

| Parameter          | Type          | Description                                                                 | Default Value        |
|--------------------|---------------|-----------------------------------------------------------------------------|----------------------|
| `@aws_cmd`         | `nvarchar(max)` | The AWS CLI command to execute (e.g., `'s3 ls'`). Required.                  | None                 |
| `@profile`         | `varchar(400)`  | AWS CLI profile name. If starts with '@', it resolves using `[dbo].[fn_get_para_value]`. | empty string |
| `@region`          | `varchar(50)`   | AWS region for the command (e.g., `'us-east-1'`).                           | empty string  |
| `@return_temp_table` | `varchar(128)` | Name of the temporary table to store results.                                | '#tmp_result'      |
| `@error_msg`       | `nvarchar(max)` | Output parameter for error messages.                                        | empty string  |
| `@aws_result`      | `nvarchar(max)` | Output parameter for the JSON result of the AWS CLI command.                | empty JSON  |

## Return Value

- Returns an integer from the `up_call_os_cmd` procedure, indicating success (0) or failure (non-zero).
- If `@aws_cmd` is null or empty, returns `-100` and sets `@error_msg` to `'@aws_cmd is null or empty!'`.

## Description

This stored procedure facilitates executing AWS CLI commands by:
1. Validating the `@aws_cmd` parameter to ensure it is not null or empty.
2. Resolving the `@profile` parameter if it starts with '@' by calling `[dbo].[fn_get_para_value]`.
3. Appending `--profile` and `--region` to the command if provided.
4. Constructing a JSON input for `[dbo].[up_call_os_cmd]` with the temporary table name and command arguments.
5. Executing the command via `[dbo].[up_call_os_cmd]` and returning its results and error messages.

## Example

```sql
DECLARE @error_msg nvarchar(max)
DECLARE @aws_result nvarchar(max)
if object_id('tempdb..#tmp_aws_output') is not null
     drop table #tmp_aws_output
create table #tmp_aws_output (run_id uniqueidentifier)

EXEC @return = [dbo].[up_execute_aws_cli]
    @aws_cmd = 's3 ls',
    @profile = 'my-profile',
    @region = 'us-west-2',
    @return_temp_table = '#tmp_aws_output',
    @error_msg = @error_msg OUTPUT,
    @aws_result = @aws_result OUTPUT
select * from #tmp_aws_output
```

## Notes

- Ensure `aws.exe` is accessible on the SQL Server machine and properly configured.
- The `@profile` parameter supports dynamic resolution using `[dbo].[fn_get_para_value]` if prefixed with '@'.
- The procedure relies on `[dbo].[up_call_os_cmd]` for command execution, which must be configured correctly.
- Results are stored in the specified `@return_temp_table` and returned as JSON in `@aws_result`.
# up_call_rest_api

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

## Stored Procedures for MS SQLServer
# up_synch_backup_history
This stored procedure synchronizes backup history data from SQL Server's `msdb` database into a `backup_history` table, filtering by specified parameters such as remote servers, backup age, and retention period. It supports all backups from servers in high availability groups. 

## Parameters

| Parameter       | Type             | Description                                                                 | Default Value |
|-----------------|------------------|-----------------------------------------------------------------------------|---------------|
| `@remote_servers` | `varchar(8000)` | Specifies the server(s) to include in the backup history synchronization. Use '*' for all servers or a comma-separated list of server names. | '*' |
| `@back_days`     | `tinyint`       | Number of days to look back for backup history to synchronize.              | 7             |
| `@keep_days`     | `int`           | Number of days to retain backup history records in the `backup_history` table. Older records are deleted. | 180           |

## Description

The `up_synch_backup_history` stored procedure retrieves backup history from `msdb.dbo.backupset` and `msdb.dbo.backupmediafamily` for databases in the `ONLINE` state with a recovery model other than `SIMPLE`. It filters backups based on the specified `@back_days` and excludes virtual devices (`device_type` not in 7). The results are merged into the `backup_history` table, and older records are deleted based on `@keep_days`.

The procedure constructs a dynamic SQL query to gather backup details, including backup set ID, log sequence numbers (LSN), dates, type, size, and device information. It uses a JSON input configuration to pass parameters to a helper procedure `[up_call_sqlfunction]`. If the helper procedure executes successfully, the results are merged into the `backup_history` table, and outdated records are removed.

## Example Usage

```sql
-- Synchronize backup history for all servers, looking back 7 days, and keeping records for 180 days
EXEC [dbo].[up_synch_backup_history] @remote_servers = '*', @back_days = 7, @keep_days = 180;

-- Synchronize backup history for specific servers, looking back 14 days, and keeping records for 90 days
EXEC [dbo].[up_synch_backup_history] @remote_servers = 'Server1,Server2', @back_days = 14, @keep_days = 90;
```

## Notes

- The procedure assumes the existence of a `backup_history` table with columns matching the fields inserted (`backup_set_id`, `first_lsn`, `last_lsn`, `backup_start_date`, `backup_finish_date`, `type`, `backup_size_kb`, `database_name`, `server_name`, `device_type`, `physical_device_name`, `family_sequence_number`).
- The helper procedure `[up_call_sqlfunction]` is called to execute the dynamic SQL and return results to a temporary table `#tmp_result`.
- The `JSON_MODIFY` function is used to configure input parameters for `[up_call_sqlfunction]`.
- The procedure uses `MERGE` to upsert records into `backup_history` and deletes records older than `@keep_days`.
- Error handling is minimal; the procedure checks the return value of `[up_call_sqlfunction]` but does not explicitly handle specific errors.

# up_WhoIsActive_alert
This stored procedure monitors SQL Server performance metrics using `sp_WhoIsActive` to detect high CPU usage, excessive active requests, or long-running blocking sessions. It generates alerts and sends email notifications with detailed reports when thresholds are exceeded. The procedure also maintains a history of alerts and performance data, with configurable retention periods.

## Parameters

| Parameter       | Type             | Description                                                                 | Default Value |
|-----------------|------------------|-----------------------------------------------------------------------------|---------------|
| `@remote_servers` | `nvarchar(4000)` | Specifies the server(s) to monitor for performance issues. Use '*' for all servers or a comma-separated list of server names. | '*' |
| `@recipient`     | `varchar(8000)` | Email address(es) to receive alert notifications, separated by semicolons.   | 'dba@example.com;' |
| `@keep_days`     | `int`           | Number of days to retain performance data in the `WhoIsActive` table. Older records are deleted. | 35 |
| `@alert_para`    | `varchar(max)`  | JSON string defining alert thresholds and counts for triggering notifications. Must include `name`, `threshold`, and `counts` for each alert type (e.g., `High_CPU`, `Active_Requests`, `Blocks_Minutes`). | '{"alerts":[{"name":"High_CPU","threshold":85,"counts":2},{"name":"Active_Requests","threshold":100,"counts":3},{"name":"Blocks_Minutes","threshold":5,"counts":1}]}' |

## Description

The `up_WhoIsActive_alert` stored procedure monitors SQL Server performance by executing `sp_WhoIsActive` (or a custom implementation via `[up_call_sqlfunction]`) to collect data on CPU usage, active requests, and blocking sessions. It evaluates these metrics against thresholds defined in the `@alert_para` JSON input and generates email alerts for new, continuing, or resolved issues. The procedure stores performance data in the `WhoIsActive` table and alert history in the `alert_hist` table, with cleanup based on `@keep_days`.

Key features:
- **Alert Types**:
  - `High_CPU`: Triggers when CPU usage exceeds the specified threshold (e.g., 85%) for a defined number of occurrences.
  - `Active_Requests`: Triggers when the number of active requests exceeds the threshold (e.g., 100) for a defined number of occurrences.
  - `Blocks_Minutes`: Triggers when blocking sessions persist longer than the threshold (e.g., 5 minutes) for a defined number of occurrences.
- **Dynamic SQL**: Uses `sp_WhoIsActive` or a custom SQL query to collect performance data, executed via `[up_call_sqlfunction]`.
- **Email Notifications**: Sends HTML-formatted emails with summary and detailed tables for triggered alerts, using `msdb.dbo.sp_send_dbmail`.
- **Temporary Tables**:
  - `#tmp_whoisactive`: Stores raw performance data from `sp_WhoIsActive`. The `run_id` column is modified to add a `servername` column if not present.
  - `#tmp_alert`: Stores formatted alert data for email reports.
  - `@alerts`, `@current_alerts`, `@alert_summary`, `@alert_list`: Table variables for processing alert thresholds and performance metrics.
- **Data Retention**: Deletes records from `WhoIsActive` older than `@keep_days`.

The procedure validates the `@alert_para` JSON input, populates alert thresholds, and processes performance data for each server. It generates alerts for new issues, resets alerts when conditions are resolved, and tracks continuing alerts. Emails include color-coded tables highlighting problematic metrics (e.g., red for high CPU, orange for blocks).

## Example Usage

```sql
-- Monitor all servers with default thresholds and recipient
EXEC [dbo].[up_WhoIsActive_alert] @remote_servers = '*', @recipient = 'dba@example.com;', @keep_days = 35;

-- Monitor specific servers with custom thresholds
EXEC [dbo].[up_WhoIsActive_alert] 
    @remote_servers = 'Server1,Server2',
    @recipient = 'admin@example.com;dba@example.com',
    @keep_days = 60,
    @alert_para = '{"alerts":[{"name":"High_CPU","threshold":90,"counts":2},{"name":"Active_Requests","threshold":50,"counts":2},{"name":"Blocks_Minutes","threshold":10,"counts":1}]}';
```

## Notes

- **Dependencies**:
  - Requires `sp_WhoIsActive` (from [GitHub](https://github.com/amachanic/sp_whoisactive)) or a compatible implementation in `[up_call_sqlfunction]`.
  - Assumes the existence of `serverlist`, `dblist`, `alert_state`, `alert_hist`, and `WhoIsActive` tables with appropriate schemas.
  - Uses `msdb.dbo.sp_send_dbmail` for email notifications, requiring Database Mail to be configured.

- **Performance Considerations**: Running `sp_WhoIsActive` from local server may impact performance. Consider deploying `sp_WhoIsActive` on remote servers for efficiency.

- **Alert Logic**:
  - Alerts are triggered when thresholds are met for the specified number of occurrences (`counts`).
  - Alerts are reset when conditions no longer apply, and a reset email is sent.
  - Continuing alerts are tracked to avoid redundant notifications.

- **Retention**: Data in `WhoIsActive` is deleted after `@keep_days` to manage storage.

# up_run_as_job

This stored procedure creates and executes a SQL Server Agent job to run a specified SQL command asynchronously. It supports logging job execution details, stopping existing jobs, and debugging options. The procedure is designed to handle dynamic SQL execution in a specified database, with automatic job cleanup and error handling.

## Parameters

| Parameter         | Type             | Description                                                                 | Default Value |
|-------------------|------------------|-----------------------------------------------------------------------------|---------------|
| `@sql`            | `nvarchar(max)`  | The SQL command to execute as a job step. Must be a valid SQL statement.     | None (required) |
| `@running_token`  | `varchar(100)`   | A unique identifier for the job, used to track and stop existing jobs.       | '' |
| `@subsystem`      | `varchar(100)`   | The SQL Server Agent subsystem to use for the job step (e.g., 'TSQL').       | 'TSQL' |
| `@jobname`        | `varchar(200)`   | The name of the job. If not provided, a unique name is generated using `@running_token` and a GUID. | '' (auto-generated) |
| `@database`       | `varchar(200)`   | The database in which to execute the SQL command.                            | 'master' |
| `@stop`           | `bit`            | If set to 1, stops and deletes an existing job with the specified `@running_token`. | 0 |
| `@log`            | `bit`            | If set to 1, logs job execution details to the `auto_job_log` table.        | 1 |
| `@debug`          | `bit`            | If set to 1, disables automatic job deletion after completion for debugging purposes. | 0 |

## Description

The `up_run_as_job` stored procedure creates a SQL Server Agent job to execute a provided SQL command (`@sql`) asynchronously in the specified database (`@database`). It supports dynamic job naming, logging of execution details, and the ability to stop existing jobs based on a `running_token`. The procedure uses SQL Server Agent's `sp_add_job`, `sp_add_jobstep`, `sp_update_job`, and `sp_add_jobserver` to configure and start the job, with optional logging to the `auto_job_log` table.

Key features:
- **Job Creation**: Creates a temporary SQL Server Agent job with a unique name, either user-specified (`@jobname`) or auto-generated using `@running_token` and a GUID.
- **Asynchronous Execution**: Executes the provided `@sql` command as a job step in the specified `@subsystem` (default: TSQL) and `@database`.
- **Job Stopping**: If `@stop = 1` and a job with the matching `@running_token` exists, the procedure deletes the job and waits 20 seconds before proceeding.
- **Logging**: If `@log = 1` and the `auto_job_log` table exists, logs job details (job name, database, command, and running token) and updates the log with start/end times or error messages.
- **Error Handling**: Validates inputs (e.g., non-empty `@sql`), checks for duplicate job names, and handles errors during job creation or execution. Returns specific error codes (`-100`, `-200`) for different failure scenarios.
- **Cleanup**: Deletes log entries in `auto_job_log` older than 60 days if logging is enabled. Jobs are automatically deleted after completion unless `@debug = 1`.
- **Dynamic SQL for Logging**: If logging is enabled, wraps the `@sql` command in a try-catch block to capture errors and update the `auto_job_log` table.

The procedure uses a transaction to ensure atomicity during job creation and rolls back on failure. It raises errors for invalid inputs (e.g., empty `@sql` or duplicate `@jobname`) and logs failures to `auto_job_log` if applicable.

## Example Usage

```sql
-- Run a simple SQL command as a job in the master database
EXEC [dbo].[up_run_as_job] 
    @sql = N'SELECT * FROM sys.tables',
    @running_token = 'TestRun_001',
    @database = 'master';

-- Run a command with logging and a custom job name
EXEC [dbo].[up_run_as_job] 
    @sql = N'EXEC sp_who',
    @running_token = 'Monitor_002',
    @jobname = 'CustomJob_002',
    @database = 'msdb',
    @log = 1;

-- Stop an existing job with a specific running token
EXEC [dbo].[up_run_as_job] 
    @sql = N'SELECT 1',
    @running_token = 'TestRun_001',
    @stop = 1;
```

## Notes

- **Dependencies**:
  - Requires SQL Server Agent to be running and accessible.
  - Assumes the existence of the `auto_job_log` table (if `@log = 1`) with columns for `jobname`, `dbname`, `commands`, `running_token`, `createdate`, `endtime`, and `error_msg`.
  - Uses system stored procedures (`msdb.dbo.sp_add_job`, `sp_add_jobstep`, `sp_update_job`, `sp_add_jobserver`, `sp_start_job`, `sp_delete_job`).
- **Error Handling**:
  - Returns `-100` for invalid inputs (empty `@sql`, duplicate job name, or job creation failure).
  - Returns `-200` if the job fails to start.
  - Returns `1` if a job with the specified `@running_token` is still running and `@stop = 0`.
  - Returns `0` on successful job creation and start.
- **Job Deletion**: Jobs are deleted automatically after completion (`@delete_level = 3`) unless `@debug = 1` (sets `@delete_level = 0`).
- **Logging**: If `@log = 1`, job execution details are stored in `auto_job_log`. Errors during execution or job creation are logged with appropriate error messages.
- **Performance Considerations**: Creating and running SQL Server Agent jobs may introduce overhead, especially for frequent executions. Ensure SQL Server Agent is configured appropriately.

- **Retention**: Log entries in `auto_job_log` are deleted after 60 days if logging is enabled.

# up_asynch_task_refresh

This stored procedure manages the asynchronous execution of tasks and task steps defined in the `automation_requests` and `task_steps` tables. It supports both local and remote task execution, with options for running tasks as SQL Server Agent jobs, handling task status updates, and logging errors. The procedure is designed to process tasks based on priority and execution timing, with support for debugging. It will process long-run db tasks by asynchronous execution of a SQL agnet job. Also it can hanlde the automated db tasks cross multi regions in predefined task steps and return the results and errors in all task steps. Nomrally this procedure needs to be scheduled to run, then it can start new tasks, check running tasks and refresh the status for long-time running tasks automatically. It's a core component to manage and implement automated db tasks based on multi db instances, multi envornments and regions in distributed system. 

## Parameters

| Parameter              | Type             | Description                                                                 | Default Value |
|------------------------|------------------|-----------------------------------------------------------------------------|---------------|
| `@refresh_Requestid`   | `int`            | The ID of the specific request to process. If NULL, processes all eligible requests. | NULL |
| `@refresh_step_id`     | `int`            | The ID of the specific task step to process. If NULL, processes all eligible steps. | NULL |
| `@debug`               | `bit`            | If set to 1, enables debug mode, printing the stored procedure definition and JSON output instead of executing. | 0 |

## Description

The `up_asynch_task_refresh` stored procedure handles the asynchronous execution of tasks and task steps defined in the `automation_requests` and `task_steps` tables. It supports both local and remote task execution, with tasks either executed directly via dynamic SQL or as SQL Server Agent jobs using the `up_run_as_job` procedure. The procedure processes tasks based on priority, execution timing, and status, updating task statuses and logging results in JSON format.

Key features:
- **Task Selection**: Identifies eligible tasks from `automation_requests` and `task_steps` based on conditions such as `enabled=1`, `asynch=1` or non-empty `return_json.return_file`, and timing constraints (`startdate`, `last_check`, `check_interval_ss`). Tasks are prioritized by the `priority` column and `last_check` or `startdate`.
- **Remote Task Handling**: If `@refresh_Requestid` is NULL and cross-region tasks exist, calls `up_sync_tasks_control_file` and `up_remote_tasks_control` to synchronize tasks between remote regions.
- **Asynchronous Execution**: For tasks with `refresh_by_job=1`, invokes `up_run_as_job` to execute the task as a SQL Server Agent job with a unique `running_token`. Otherwise, executes the task directly using `sp_executesql`.
- **Status Management**: Updates task statuses in `automation_requests` or `task_steps` to `running`, `success`, `failed`, `Waiting_sync`, or `synced` based on execution outcomes. Updates `enddate` and `return_json` accordingly.
- **Error Handling**: Uses try-catch blocks to capture and log errors. Errors are stored in `return_json` with a message and logged via `up_log_error`. Returns `-100` on failure.
- **Debugging**: If `@debug=1`, prints the stored procedure definition and JSON output instead of executing the task.
- **Task Step Management**: For tasks in `task_steps`, ensures steps are processed sequentially by deleting steps from the checklist if earlier steps are still pending.
- **JSON Output**: Stores execution results and status in the `return_json` column, using `JSON_MODIFY` to update fields like `status`, `step_id`, and `msg`.

The procedure processes tasks in a loop, selecting one task or step at a time, executing it, updating its status, and removing it from the checklist until no tasks remain. It ensures atomicity for direct executions using try-catch and delegates job-based executions to `up_run_as_job`.

## Example Usage

```sql
-- Process all eligible tasks and steps
EXEC [dbo].[up_asynch_task_refresh];

-- Process a specific request
EXEC [dbo].[up_asynch_task_refresh] 
    @refresh_Requestid = 1001;

-- Process a specific task step
EXEC [dbo].[up_asynch_task_refresh] 
    @refresh_Requestid = -2001, 
    @refresh_step_id = 1;

-- Run in debug mode to print task details
EXEC [dbo].[up_asynch_task_refresh] 
    @debug = 1;
```

## Notes

- **Dependencies**:
  - Requires access to `automation_requests` and `task_steps` tables with appropriate schemas.
  - Uses `up_run_as_job` for job-based execution.
  - Calls `up_sync_tasks_control_file` and `up_remote_tasks_control` for cross-region task synchronization.
  - Uses `fn_get_para_value` to retrieve configuration parameters like `@site_name` and `@tasks_master`.
  - Relies on system stored procedure `sp_executesql` for dynamic SQL execution.
- **Error Handling**:
  - Returns `-100` for errors, with details logged via `up_log_error` and stored in `return_json`.
  - Captures errors during direct execution and updates `return_json` with error messages.
- **Status Values**:
  - `running`: Task or step is currently executing.
  - `success`: Task or step completed successfully.
  - `failed`: Task or step failed.
  - `Waiting_sync`: Task or step completed and awaiting synchronization (for remote tasks).
  - `synced`: Task or step has been synchronized.
- **Performance Considerations**:
  - Processing tasks in a loop may impact performance for large task sets. Ensure `check_interval_ss` is tuned to avoid excessive checks.
  - Job-based execution via `up_run_as_job` introduces overhead due to SQL Server Agent job creation.
- **Debugging**:
  - Use `@debug=1` to inspect stored procedure definitions and JSON output without executing tasks.

- **Task Steps**:
  - Steps in `task_steps` are processed sequentially, with earlier steps blocking later ones if still pending.
  - Negative `requestid` values in the checklist correspond to `task_id` from `task_steps`.
- **JSON Handling**:
  - The `return_json` column stores execution results and status, with fields like `status`, `step_id`, and `msg` updated dynamically.
  
