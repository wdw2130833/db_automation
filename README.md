# Database Automation Platform

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

## Getting Started

For detailed installation, setup instructions and usage examples, refer to the [installation guide](installation.md).

### Prerequisites
- **Microsoft SQL Server 2016 or higher** with CLR enabled and .NET Framework 4.7.2 or higher.
- **MySQL ODBC driver** for MySQL support.
- **PostgreSQL ODBC driver** for PostgreSQL support.

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
