# Database Automation Platform

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
  - [Core stored procedures](#Core-stored-procedures)
    - [up_call_sqlfunction](#up_call_sqlfunction)
    - [up_call_os_cmd](#up_call_os_cmd)
    - [up_execute_ps](#up_execute_ps)
    - [up_execute_aws_cli](#up_execute_aws_cli)
    - [up_call_rest_api](#up_call_rest_api)
  - [Advanced Usage](#advanced-usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project provides a centralized platform to build database applications running on **Microsoft SQL Server**, focusing on automating data processes in a distributed infrastructure system. It enables seamless implementation of complex data flows and processes at the backend using **TSQL**, supporting **MSSQL**, **MySQL**, and **PostgreSQL**, with extensibility to other database systems.

## Overview

The platform simplifies the development of database applications for integrating and processing data across distributed infrastructures. It allows users to write SQL queries that execute on remote database servers, enabling complex database tasks across thousands of databases on hundreds of instances in multiple global regions in minutes.
It is designed to execute SQL functions or queries as batches across multiple SQL Server, MySQL, or PostgreSQL database instances in a distributed environment. It supports flexible targeting of servers, databases, instance types, and environments, with options for dry runs, debugging, logging, and error handling. This platform also integrates REST API or CLI on AWS/Azure/GCP Cloud service and external systems (e.g., Jira and Salesforce) for devops tasks, logging task outcomes and supports JSON-based input and output for configuration and results. It provides a robust mechanism to execute SQL commands, manage parallelism, and track execution status. 

### Key Features

#### Central Management Database Tasks
- Generate Excel or stats reports based ad-hoc queries from all customers' databases in a distributed environment.
- Query configuration settings for development teams before building or deploying new features.
- Apply hotfix scripts to all customers' databases.
- Collect performance statistics and build monitoring/alert systems.
- Generate audit reports.
- Implement standard maintenance jobs for DBAs from a central database.
- Integrates with Jira, Salesforce ticket systems or Teams and Slack messagers using REST APIs.

#### Automated Data Process Tasks
- Copy application databases from production to development or QA environments with data encryption/decryption and scrubbing via scheduled jobs.
- Refresh application databases from production to training, demo, or test environments, including data scrubbing, backup/restore/transfer, and archiving for large databases (>50 TB), plus application setting backup/recovery.
- Migrate customer databases for integration projects between customers and internally hosted applications.

## Supported Databases
- **Microsoft SQL Server**
- **MySQL**
- **PostgreSQL**
- Extensible to other database systems with appropriate ODBC drivers.

## Installation

For detailed installation and setup instructions, refer to the [installation guide](installation.md).

### Prerequisites
- **Microsoft SQL Server 2016 or higher** with CLR enabled and .NET Framework 4.7.2 or higher.
- **MySQL ODBC driver** for MySQL support.
- **PostgreSQL ODBC driver** for PostgreSQL support.

## Usage

### Core stored procedures

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
# up_execute_ps 

## Overview
The `up_execute_ps` stored procedure executes PowerShell scripts by invoking the `powershell.exe` command through the `[dbo].[up_call_os_cmd]` procedure. It handles script execution, error handling, and returns results in a JSON format or a temporary table.

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

EXEC @return = [dbo].[up_execute_ps]
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

### Advanced Usage
   This is for test
### Example Use Cases
1. **Generate a Report**:
   - Query active logins across all customer databases and export to Excel.
2. **Automate Data Refresh**:
   - Schedule a job to copy and scrub production data to a test environment.
3. **Apply Hotfixes**:
   - Deploy SQL scripts to thousands of databases simultaneously.
4. **Monitor Performance**:
   - Collect and analyze performance stats from distributed database instances.

## Extensibility
The platform is designed to easily integrate additional database systems by configuring appropriate ODBC drivers and updating the `serverlist` table with connection details.

## Contributing
Contributions are welcome! Please submit pull requests or open issues for bug reports, feature requests, or documentation improvements.

## License
This project is licensed under the [MIT License](./LICENSE).
