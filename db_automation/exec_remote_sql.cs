using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Data.Odbc;
using System.Threading;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using System.Text;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Xml.Linq;
using System.Collections.Specialized;
using static System.Net.Mime.MediaTypeNames;
using System.Transactions;
using System.Data.Common;
using System.Net.NetworkInformation;
using System.Net;
using System.Security.Policy;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Runtime.Remoting.Messaging;


public partial class StoredProcedures
{
    const string MSSQL = "MSSQL";
    const string MySQL = "MySQL";
    const string PGSQL = "PostgreSQL";
    const string success = "success";
    const string failed = "failed";
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static Int32 exec_remote_sql(SqlString Temp_Folder, out SqlString Return_Msg, Int32 max_threads, Boolean debug)
    {
        Return_Msg = success;
        string SQL = "";
        string Return_TMP_Table = "";
        var Exec_Results = new List<Return_DT>();
        var Exec_Lists = new List<remote_execution>();

        SqlPipe pipe = SqlContext.Pipe;
        SqlConnection oConn = new SqlConnection();
        SqlCommand oCmd = new SqlCommand();
        oConn = new SqlConnection("context connection = true;");
        oConn.Open();

        SQL = "if object_id('tempdb..#remote_exec_content') is null " +
            " raiserror ('Can not find #remote_exec_content!',16,1)" +
            " select * from #remote_exec_content order by return_tmp_table; ";
        oCmd.Connection = oConn;
        oCmd.CommandText = SQL;
        if (debug) { pipe.ExecuteAndSend(oCmd); }
        //pickup remote sqls... 
        try
        {
            Return_DT This_Result;
            SqlDataReader reader = oCmd.ExecuteReader();

            while (reader.Read())
            {
                var This_Execution = new remote_execution();
                This_Execution.Run_ID = reader.GetGuid(reader.GetOrdinal("Run_ID"));
                This_Execution.Threads = reader.GetInt32(reader.GetOrdinal("Threads"));
                This_Execution.TimeOut = reader.GetInt32(reader.GetOrdinal("timeout"));

                This_Execution.Servername = reader.GetString(reader.GetOrdinal("servername"));
                This_Execution.IP_or_DNS = reader.GetString(reader.GetOrdinal("IP_or_DNS"));
                This_Execution.DBname = reader.GetString(reader.GetOrdinal("dbname"));
                This_Execution.SQL = reader.GetString(reader.GetOrdinal("TheSQL"));
                This_Execution.Port = reader.GetString(reader.GetOrdinal("Port"));
                This_Execution.DBProvider = reader.GetString(reader.GetOrdinal("db_provider"));
                This_Execution.Driver = reader.GetString(reader.GetOrdinal("DB_Driver"));
                This_Execution.UserName = reader.GetString(reader.GetOrdinal("username"));
                This_Execution.PWD = reader.GetString(reader.GetOrdinal("pwd"));
                This_Execution.Remote_Output = reader.GetString(reader.GetOrdinal("Remote_Output"));
                This_Execution.CMD_Type = reader.GetString(reader.GetOrdinal("CMD_Type"));
                This_Execution.Arguments = reader.GetString(reader.GetOrdinal("Arguments"));
                This_Execution.Connection_Options = reader.GetString(reader.GetOrdinal("Connection_Options"));
                This_Execution.Resturn_TMP_Table = reader.GetString(reader.GetOrdinal("return_tmp_table"));
                This_Execution.Debug = reader.GetBoolean(reader.GetOrdinal("Debug"));
                This_Execution.Status = "Waiting";
                This_Execution.Remote_Error = "";
                Exec_Lists.Add(This_Execution);
                if (This_Execution.CMD_Type !="")
                {
                    This_Result = new Return_DT { Result_DT = new ConcurrentBag<DataTable>(), Executions = new List<Thread>() };
                    This_Result.Result_Name = "CMD";
                    This_Result.Waitings = 0;
                    This_Result.Runnings = 0;
                    This_Result.Stops = 0;
                    This_Result.Threads = This_Execution.Threads;
                    Exec_Results.Add(This_Result);
                }
                else if (Return_TMP_Table != This_Execution.Resturn_TMP_Table)
                {
                    This_Result = new Return_DT { Result_DT = new ConcurrentBag<DataTable>(), Executions = new List<Thread>() };
                    Return_TMP_Table = This_Execution.Resturn_TMP_Table;
                    This_Result.Result_Name = Return_TMP_Table;
                    This_Result.Waitings = 0;
                    This_Result.Runnings = 0;
                    This_Result.Stops = 0;
                    This_Result.Threads = This_Execution.Threads;
                    Exec_Results.Add(This_Result);
                }

            }
            reader.Close();
        }
        catch (SqlException ex)
        {
            SqlErrorCollection myError = ex.Errors;
            string Errmsg = ex.Message;
            for (int i = 0; i <= myError.Count - 1; i++)
                Errmsg = Errmsg + myError[i].ToString();
            Return_Msg = "SQL Error:" + Errmsg;
            pipe.Send(Return_Msg.ToString());
            return -200;
        }
        catch (Exception ex)
        {
            Return_Msg = "Exception Error:" + ex.Message;
            pipe.Send(Return_Msg.ToString());
            return -300;
        }
        //start to execute SQL on remote servers... 
        try
        {
            int iCurrentThreads = 0;
            int Total_Waitings = Exec_Lists.Count;
            DateTime currenttime = DateTime.Now;

            foreach (Return_DT This_Return in Exec_Results)
            {
                foreach (remote_execution This_Execution in Exec_Lists)
                {
                    if (This_Execution.Resturn_TMP_Table == This_Return.Result_Name || This_Return.Result_Name == "CMD")
                    { This_Return.Waitings = This_Return.Waitings + 1; }
                }
            }
            while (Total_Waitings > 0)
            {
                iCurrentThreads = 0;
                foreach (Return_DT This_Return in Exec_Results)
                {
                    if (This_Return.Waitings > 0)
                    {
                        foreach (remote_execution This_Execution in Exec_Lists)
                        {
                            if (This_Return.Result_Name == "CMD" && This_Execution.Status == "Waiting" && This_Return.Runnings < This_Return.Threads)
                            {
                                var executer_cmd = new ExecuteCMD(Temp_Folder.ToString(), This_Execution);
                                Thread oItem = new System.Threading.Thread(executer_cmd.Process);
                                oItem.Name = This_Execution.Run_ID.ToString();
                                oItem.Start();
                                This_Return.Executions.Add(oItem);
                                if (This_Execution.Debug) { pipe.Send("Sent query to " + This_Execution.Servername + "--" + This_Execution.DBname); }
                                This_Execution.Status = "Running";
                                Total_Waitings = Total_Waitings - 1;
                                This_Return.Waitings = This_Return.Waitings - 1;
                                This_Return.Runnings = This_Return.Runnings + 1;
                                if (iCurrentThreads % 50 == 0)
                                {
                                    Thread.Sleep(2000);
                                }
                                if (This_Return.Runnings >= This_Return.Threads)
                                {
                                    string msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Started " + (This_Return.Runnings + This_Return.Stops).ToString() + " executions for " + This_Return.Result_Name + ", " + This_Return.Waitings.ToString() + " quries are waiting!";
                                    pipe.Send(msg);
                                    Thread.Sleep(3000);
                                }
                            }
                            else if (This_Execution.Resturn_TMP_Table == This_Return.Result_Name && This_Execution.Status == "Waiting" && This_Return.Runnings < This_Return.Threads)
                            {
                                var executer = new ExecuteSQL(This_Execution, This_Return.Result_DT);
                                Thread oItem = new System.Threading.Thread(executer.Process);
                                oItem.Name = This_Execution.Run_ID.ToString();
                                oItem.Start();
                                This_Return.Executions.Add(oItem);
                                if (This_Execution.Debug) { pipe.Send("Sent query to " + This_Execution.Servername + "--" + This_Execution.DBname); }
                                This_Execution.Status = "Running";
                                Total_Waitings = Total_Waitings - 1;
                                This_Return.Waitings = This_Return.Waitings - 1;
                                This_Return.Runnings = This_Return.Runnings + 1;
                                iCurrentThreads = iCurrentThreads + 1;
                                if (iCurrentThreads % 50 == 0)
                                {
                                    Thread.Sleep(2000);
                                }
                                if (This_Return.Runnings >= This_Return.Threads)
                                {
                                    string msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Started " + (This_Return.Runnings + This_Return.Stops).ToString() + " executions for " + This_Return.Result_Name + ", " + This_Return.Waitings.ToString() + " quries are waiting!";
                                    pipe.Send(msg);
                                    Thread.Sleep(3000);
                                }
                            }
                        }
                        RefreshThreads(This_Return);
                    }
                }
                Thread.Sleep(10000);
            }
            if (Exec_Lists.Count > 1)
            {
                string msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Sent out " + Exec_Lists.Count.ToString() + " queries to remote servers!";
                pipe.Send(msg);
                Thread.Sleep(3000);
            }
            currenttime = DateTime.Now;
            int ThreadsLeft = RunningThreads(Exec_Results);
            while (ThreadsLeft > 0)
            {
                ThreadsLeft = RunningThreads(Exec_Results);
                if ((DateTime.Now - currenttime).TotalSeconds > 20)
                {
                    currenttime = DateTime.Now;
                    string msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Running queries: " + ThreadsLeft.ToString();
                    pipe.Send(msg);
                }
                Thread.Sleep(1000);
            }
            //start to process the results from remote sql... 
            pipe.Send(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Start to return results.");
            pipe.Send(Environment.NewLine);
            DataTable All_DT = new DataTable();

            foreach (Return_DT This_Return in Exec_Results)
            {
                while (This_Return.Result_DT.Count > 0)
                {
                    DataTable TheReader;
                    if (This_Return.Result_DT.TryTake(out TheReader))
                    {
                        if (TheReader.Rows.Count > 0)
                        {
                            if (This_Return.Result_Name.StartsWith("#"))  // return data back into temp table
                            {
                                Return_Msg = Alter_Temp_Table(This_Return.Result_Name, TheReader, oConn);
                                if (Return_Msg != "success") { return -10; }
                                Return_Msg = LoadDataTable(This_Return.Result_Name, TheReader, oConn);
                                if (Return_Msg != "success") { return -10; }
                            }
                            else   //return back directly
                            {
                                try
                                {
                                    All_DT.Merge(TheReader, true);
                                }
                                catch (Exception ex)
                                {
                                    Return_Msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---" + This_Return.Result_Name + "---data merge Error:" + ex.Message;
                                    SqlContext.Pipe.Send(Return_Msg.ToString());
                                    return -30;
                                }
                            }
                        }
                    }
                    else
                    {
                        Return_Msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---" + This_Return.Result_Name + "---concurrentBag tryout DataTable failed";
                        SqlContext.Pipe.Send(Return_Msg.ToString());
                        return -5;
                    }
                }
            }
            if (All_DT.Rows.Count > 0) { Return_Msg = SendDataTable(All_DT); }
            Return_Msg = Update_Exec_Content(Exec_Lists, oConn);
            pipe.Send(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " : Returned all results");
            pipe.Send(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " : " + Return_Msg.ToString());
            oCmd.Dispose();
            oConn.Close();
            oConn.Dispose();
            if (Return_Msg.ToString() != success)
            {
                return -1;
            }
            Return_Msg = success;
            return 0;
        }
        catch (SqlException sqlex)
        {
            Return_Msg = "Exception_SQL: " + sqlex.Message;
            SqlContext.Pipe.Send(Return_Msg.ToString());
            return -20;
        }
        catch (System.Transactions.TransactionAbortedException ex)
        {

            Return_Msg = "Exception_Trans_Aborted:" + ex.Message;
            SqlContext.Pipe.Send(Return_Msg.ToString());
            return -25;
        }
        catch (ApplicationException ex)
        {
            Return_Msg = "Exception_App:" + ex.Message;
            SqlContext.Pipe.Send(Return_Msg.ToString());
            return -30;
        }
        catch (Exception ex)
        {
            string Return_Errmsg = ex.Message;
            if (!Return_Errmsg.Contains("exceeds maximum length supported of 4000"))
            {
                Return_Msg = "Exception: " + Return_Errmsg;
                SqlContext.Pipe.Send(Return_Msg.ToString());
                return -50;
            }
            return -55;
        }
    }
    [SqlFunction(IsDeterministic = true, IsPrecise = true)]
    public static SqlBoolean RegEx_IsMatch(SqlString Text, SqlString Expression)
    {
        return Regex.IsMatch(Text.Value, Expression.Value, RegexOptions.IgnoreCase, new TimeSpan(0, 0, 30));
    }
    [SqlFunction(IsDeterministic = true, IsPrecise = true)]
    public static SqlString readfile(string filename)
    {
        string thedata = "";
        try
        {
            if (string.IsNullOrEmpty(filename))
            {
                thedata = "Error: file name cannot be null or empty.";
                SqlContext.Pipe.Send(thedata.ToString());
                return thedata;
            }

            if (!File.Exists(@filename))
            {
                thedata = "Error: file not exists.";
                SqlContext.Pipe.Send(thedata.ToString());
                return thedata;
            }
            thedata = File.ReadAllText(@filename);
            return thedata;
        }
        catch (UnauthorizedAccessException ex)
        {
            thedata = $"Error:Permission denied accessing file: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return thedata;
        }
        catch (IOException ex)
        {
            thedata = $"Error:IO error writing to file: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return thedata;
        }
        catch (Exception ex)
        {
            thedata = $"Error:Unexpected error: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return thedata;
        }
    }
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static Int32 call_rest_api(string api_url, SqlString content, string headers, string method, string content_type, string encode, string accept, string useragent, out SqlString api_return)
    {

        api_return = "";
        string jsonPayload = content.ToString();
        try
        {
            ServicePointManager.CertificatePolicy = new CustomCertificatePolicy();
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            System.Net.ServicePointManager.Expect100Continue = true;

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(api_url);
            request.Method = method;
            if (!string.IsNullOrEmpty(content_type))
            {
                request.ContentType = content_type;
            }
            if (!string.IsNullOrEmpty(useragent))
            {
                request.UserAgent = useragent;
            }
            if (!string.IsNullOrEmpty(accept))
            {
                request.Accept = accept;
            }
            request.Timeout = 30000;
            if (!string.IsNullOrEmpty(headers))
            {
                string[] the_headers = headers.Split(';');

                for (int i = 0; i < the_headers.Length; i++)
                {
                    string[] thisHeader = the_headers[i].Split('=');
                    request.Headers[thisHeader[0]] = thisHeader[1];
                }
            }
            if (method == "POST")
            {
                byte[] payloadBytes = Encoding.UTF8.GetBytes(jsonPayload);
                request.ContentLength = payloadBytes.Length;
                using (Stream requestStream = request.GetRequestStream())
                {
                    requestStream.Write(payloadBytes, 0, payloadBytes.Length);
                }
            }

            HttpWebResponse postResponse = (HttpWebResponse)request.GetResponse();
            StreamReader postReqReader;

            if (!string.IsNullOrEmpty(encode))
            {
                postReqReader = new StreamReader(postResponse.GetResponseStream(), Encoding.GetEncoding(encode));
            }
            else
            {
                postReqReader = new StreamReader(postResponse.GetResponseStream());
            }

            if (postResponse.StatusCode != HttpStatusCode.OK && postResponse.StatusCode != HttpStatusCode.Created)
            {
                api_return = $"HTTP Error: {postResponse.StatusCode}, Details: {postReqReader.ReadToEnd()}";
                return -50;
            }
            api_return = api_return + postReqReader.ReadToEnd();


            return 0;
        }
        catch (WebException ex)
        {
            if (ex.Response is HttpWebResponse errorResponse)
            {
                using (Stream stream = errorResponse.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {
                    api_return = $"HTTP Error: {errorResponse.StatusCode}, Details: {reader.ReadToEnd()}";
                }
            }
            else
            {
                api_return = ex.Message;
            }

            return -100;
        }
        catch (Exception ex)
        {
            api_return = ex.Message;
            return -200;
        }
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static Int32 up_readfile(string filename, out SqlString thedata)
    {
        try
        {
            if (string.IsNullOrEmpty(filename))
            {
                thedata = "Error: Filename cannot be null or empty.";
                SqlContext.Pipe.Send(thedata.ToString());
                return -100;
            }
            thedata = File.ReadAllText(@filename);
            return 0;
        }
        catch (UnauthorizedAccessException ex)
        {
            thedata = $"Permission denied accessing file: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return -101; // Permission error
        }
        catch (IOException ex)
        {
            thedata = $"IO error writing to file: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return -201; // IO error
        }
        catch (Exception ex)
        {
            thedata = $"Unexpected error: {ex.Message}";
            SqlContext.Pipe.Send(thedata.ToString());
            return -202; // General error
        }
    }
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int WriteFile(string filename, SqlString thedata)
    {
        string errMsg = "";
        try
        {
            // Validate inputs
            if (string.IsNullOrEmpty(filename))
            {
                thedata = "Error: Filename cannot be null or empty.";
                SqlContext.Pipe.Send(thedata.ToString());
                return -100;
            }
            if (thedata.IsNull)
            {
                thedata = "Error: Input data cannot be null.";
                SqlContext.Pipe.Send(thedata.ToString());
                return -200;
            }

            // Write to file
            File.WriteAllText(filename, thedata.ToString());
            return 0; // Success
        }
        catch (UnauthorizedAccessException ex)
        {
            errMsg = $"Permission denied accessing file: {ex.Message}";
            SqlContext.Pipe.Send(errMsg);
            return -101; // Permission error
        }
        catch (IOException ex)
        {
            errMsg = $"IO error writing to file: {ex.Message}";
            thedata = errMsg;
            SqlContext.Pipe.Send(errMsg);
            return -201; // IO error
        }
        catch (Exception ex)
        {
            errMsg = $"Unexpected error: {ex.Message}";
            thedata = errMsg;
            SqlContext.Pipe.Send(errMsg);
            return -202; // General error
        }
    }
    public class CustomCertificatePolicy : ICertificatePolicy
    {
        public bool CheckValidationResult(ServicePoint srvPoint, X509Certificate certificate,
            WebRequest request, int certificateProblem)
        {
            return true; // Accept all (insecure)
        }
    }
    private static string SendDataTable(DataTable dt)
    {
        bool[] coerceToString = null; // Do we need to coerce this column to string?
        SqlMetaData[] metaData = ExtractDataTableColumnMetaData(dt, ref coerceToString);
        var record = new SqlDataRecord(metaData);
        SqlPipe pipe = SqlContext.Pipe;
        pipe.SendResultsStart(record);
        Int64 rows = 0;
        try
        {
            foreach (DataRow row in dt.Rows)
            {
                for (int index = 0, loopTo = record.FieldCount - 1; index <= loopTo; index++)
                {
                    object value = row[index];
                    if (value is null && coerceToString[index])
                    {
                        value = value.ToString();
                    }

                    record.SetValue(index, value);
                }
                rows++;
                pipe.SendResultsRow(record);
            }
            pipe.SendResultsEnd();
            pipe.Send("return Rows=" + rows.ToString());
            return "success";
        }
        catch (Exception ex)
        {
            pipe.SendResultsEnd();
            string Return_Msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Exception_SendDataTable:" + ex.Message;
            SqlContext.Pipe.Send(Return_Msg.ToString());
            return Return_Msg;
        }
    }
    private static SqlMetaData[] ExtractDataTableColumnMetaData(DataTable dt, ref bool[] coerceToString)
    {
        var metaDataResult = new SqlMetaData[dt.Columns.Count];
        coerceToString = new bool[dt.Columns.Count];
        for (int index = 0, loopTo = dt.Columns.Count - 1; index <= loopTo; index++)
        {
            DataColumn column = dt.Columns[index];
            metaDataResult[index] = SqlMetaDataFromColumn(column, ref coerceToString[index]);
        }

        return metaDataResult;
    }
    private static string Update_Exec_Content(List<remote_execution> result_contents, SqlConnection oConn)
    {
        string return_msg = success;
        using (SqlTransaction transaction = oConn.BeginTransaction())
        {
            try
            {
                string sql = "update [#remote_exec_content] set remote_output=@remote_output,remote_error=@remote_error,affected_rows=@affected_rows where run_id=@run_id";

                foreach (remote_execution This_execution in result_contents)
                {
                    if (This_execution.Remote_Error != success)
                    {
                        SqlContext.Pipe.Send(This_execution.Servername + "." + This_execution.DBname + " -- failed: " + This_execution.Remote_Error);
                        SqlContext.Pipe.Send(Environment.NewLine);
                        return_msg = failed;
                    }
                    using (SqlCommand command = new SqlCommand(sql, oConn, transaction))
                    {
                        command.Parameters.AddWithValue("@remote_output", This_execution.Remote_Output);
                        command.Parameters.AddWithValue("@remote_error", This_execution.Remote_Error);
                        command.Parameters.AddWithValue("@affected_rows", This_execution.AffectedRows);
                        command.Parameters.AddWithValue("@run_id", This_execution.Run_ID);
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
            catch (SqlException ex)
            {
                SqlErrorCollection myError = ex.Errors;
                string Errmsg = ex.Message;
                for (int i = 0; i <= myError.Count - 1; i++)
                    Errmsg = Errmsg + myError[i].ToString();
                return_msg = "SQL Error:" + Errmsg;
                SqlContext.Pipe.Send(return_msg);
                return return_msg;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                return_msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Load_Error Error:" + ex.Message;
                SqlContext.Pipe.Send(return_msg);
            }
        }
        return return_msg;
    }
    private static string Alter_Temp_Table(string tmptable_name, DataTable dt, SqlConnection oConn)
    {
        string sql = "";
        string the_length = "";
        bool[] coerceToString = null;
        List<string> data_type_no_lenth = new List<string> { "Int", "BigInt", "Bit", "DateTime", "Money", "SmallInt", "TinyInt", "UniqueIdentifier", "Ntext", "Real", "sql_variant", "DateTime", "Variant" };
        try
        {
            SqlMetaData[] metaData = ExtractDataTableColumnMetaData(dt, ref coerceToString);
            foreach (SqlMetaData column in metaData)
            {
                sql = sql + "IF COL_LENGTH('TEMPDB.." + tmptable_name + "', '" + column.Name + "') IS NULL " +
                        " ALTER TABLE [" + tmptable_name + "] add ";
                if (data_type_no_lenth.Contains(column.SqlDbType.ToString()))
                {
                    if (column.SqlDbType.ToString() == "Variant")
                    {
                        sql = sql + "[" + column.Name + "] sql_Variant;";
                    }
                    else
                    {
                        sql = sql + "[" + column.Name + "] " + column.SqlDbType.ToString() + ";";
                    }
                }
                else
                {
                    the_length = " (" + column.MaxLength.ToString().Replace("-1", "MAX") + ");";
                    if (column.SqlDbType.ToString() == "Decimal")
                    { the_length = " (" + column.Precision.ToString() + "," + column.Scale.ToString() + ");"; }
                    sql = sql + "[" + column.Name + "] " + column.SqlDbType.ToString() + the_length;
                }
            }
            //SqlContext.Pipe.Send(sql);
            using (SqlCommand command = new SqlCommand(sql, oConn))
            { command.ExecuteNonQuery(); }
            return success;
        }
        catch (Exception ex)
        {
            string error_msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---Alter_Temp_Table Error:" + ex.Message;
            SqlContext.Pipe.Send(error_msg);
            return error_msg;
        }

    }
    private static string LoadDataTable(string tmptable_name, DataTable dt, SqlConnection oConn)
    {
        List<string> columnlist = new List<string>();
        string sql = "select name from tempdb.sys.columns where object_id=object_id('tempdb.." + tmptable_name + "') order by column_id";

        SqlCommand cmd = new SqlCommand(sql, oConn);
        SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            columnlist.Add(reader.GetString(reader.GetOrdinal("name")));
        }
        reader.Close();
        cmd.Dispose();
        using (SqlTransaction transaction = oConn.BeginTransaction())
        {
            try
            {
                sql = "insert into [" + tmptable_name + "] values (";
                foreach (string column in columnlist)
                {
                    sql = sql + '@' + column + ",";
                }
                sql = sql.Substring(0, sql.Length - 1) + ")";
                foreach (DataRow row in dt.Rows)
                {
                    using (SqlCommand command = new SqlCommand(sql, oConn, transaction))
                    {
                        foreach (string column in columnlist)
                        {
                            command.Parameters.AddWithValue("@" + column, row[column] ?? DBNull.Value);
                        }
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                string error_msg = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "---insert " + tmptable_name + " Error:" + ex.Message;
                SqlContext.Pipe.Send(error_msg);
                SqlContext.Pipe.Send(sql);
                return error_msg;
            }
        }
        return success;
    }
    private static Exception InvalidDataTypeCode(TypeCode code)
    {
        return new ArgumentException("Invalid type: " + code.ToString());
    }
    private static Exception UnknownDataType(Type clrType)
    {
        return new ArgumentException("Unknown type: " + clrType.ToString());
    }
    private static string SQLServerBulkCopy(DataTable dt, string Sql, string TableName, SqlConnection conn, bool connectionTypeSQL = true)
    {
        try
        {
            string result = "{\"status:\"OK\"}";
            if (connectionTypeSQL)
            {
                if (conn.State == ConnectionState.Closed)
                    conn.Open();
                using (SqlCommand cmd = new SqlCommand(Sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
                using (SqlBulkCopy sqlcpy = new SqlBulkCopy(conn))
                {
                    sqlcpy.DestinationTableName = TableName;  //copy the datatable to the sql table
                    sqlcpy.WriteToServer(dt);
                }

                return result;
            }
            else
            {
                throw new ArgumentOutOfRangeException("This method is only for SQL Server Engines");
            }
        }
        catch (SqlException sqlex)
        {
            return "{\"status:\"failed\",\"msg:\"" + TableName + "-" + sqlex.Message + "\"}";

        }
        catch (Exception ex)
        {
            return "{\"status:\"failed\",\"msg:\"" + TableName + "-" + ex.Message + "\"}";
        }
    }
    private static SqlMetaData SqlMetaDataFromColumn(DataColumn column, ref bool coerceToString)
    {
        coerceToString = false;
        SqlMetaData sql_md = default;
        Type clrType = column.DataType;
        string name = column.ColumnName;
        switch (Type.GetTypeCode(clrType))
        {
            case TypeCode.Boolean:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.Bit);
                    break;
                }

            case TypeCode.Byte:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.TinyInt);
                    break;
                }

            case TypeCode.Char:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.NVarChar, 1);
                    break;
                }

            case TypeCode.DateTime:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.DateTime);
                    break;
                }

            case TypeCode.DBNull:
                {
                    throw InvalidDataTypeCode(TypeCode.DBNull);
                }

            case TypeCode.Decimal:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.Decimal);
                    break;
                }

            case TypeCode.Double:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.Money);
                    break;
                }

            case TypeCode.Empty:
                {
                    throw InvalidDataTypeCode(TypeCode.Empty);

                }

            case TypeCode.Int16:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.SmallInt);
                    break;
                }

            case TypeCode.Int32:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.Int);
                    break;
                }

            case TypeCode.Int64:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.BigInt);
                    break;
                }

            case TypeCode.SByte:
                {
                    throw InvalidDataTypeCode(TypeCode.SByte);

                }

            case TypeCode.Single:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.Real);
                    break;
                }

            case TypeCode.String:
                {
                    sql_md = new SqlMetaData(name, SqlDbType.NVarChar, -1);
                    break;
                }

            case TypeCode.UInt16:
                {
                    throw InvalidDataTypeCode(TypeCode.UInt16);

                }

            case TypeCode.UInt32:
                {
                    throw InvalidDataTypeCode(TypeCode.UInt32);

                }

            case TypeCode.UInt64:
                {
                    throw InvalidDataTypeCode(TypeCode.UInt64);

                }

            case TypeCode.Object:
                {
                    sql_md = SqlMetaDataFromObjectColumn(name, column, clrType);
                    if (sql_md is null)
                    {
                        // Unknown type, try to treat it as string
                        sql_md = new SqlMetaData(name, SqlDbType.NVarChar, column.MaxLength);
                        coerceToString = true;
                    }

                    break;
                }

            default:
                {
                    throw UnknownDataType(clrType);

                }
        }

        return sql_md;
    }
    private static SqlMetaData SqlMetaDataFromObjectColumn(string name, DataColumn column, Type clrType)
    {
        SqlMetaData sql_md = default;
        if (ReferenceEquals(clrType, typeof(byte[])) || ReferenceEquals(clrType, typeof(SqlBytes)) || ReferenceEquals(clrType, typeof(char[])) || ReferenceEquals(clrType, typeof(SqlChars)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.VarBinary, column.MaxLength);
        }
        else if (ReferenceEquals(clrType, typeof(SqlString)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.NVarChar, -1);
        }
        else if (ReferenceEquals(clrType, typeof(SqlBinary)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.VarBinary, -1);
        }
        else if (ReferenceEquals(clrType, typeof(Guid)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.UniqueIdentifier);
        }
        else if (ReferenceEquals(clrType, typeof(object)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Variant);
        }
        else if (ReferenceEquals(clrType, typeof(SqlBoolean)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Bit);
        }
        else if (ReferenceEquals(clrType, typeof(SqlByte)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.TinyInt);
        }
        else if (ReferenceEquals(clrType, typeof(SqlDateTime)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.DateTime);
        }
        else if (ReferenceEquals(clrType, typeof(SqlDouble)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Float);
        }
        else if (ReferenceEquals(clrType, typeof(SqlGuid)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.UniqueIdentifier);
        }
        else if (ReferenceEquals(clrType, typeof(SqlInt16)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.SmallInt);
        }
        else if (ReferenceEquals(clrType, typeof(SqlInt32)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Int);
        }
        else if (ReferenceEquals(clrType, typeof(SqlInt64)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.BigInt);
        }
        else if (ReferenceEquals(clrType, typeof(SqlMoney)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Money);
        }
        else if (ReferenceEquals(clrType, typeof(SqlDecimal)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Decimal, SqlDecimal.MaxPrecision, 0);
        }
        else if (ReferenceEquals(clrType, typeof(SqlSingle)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Real);
        }
        else if (ReferenceEquals(clrType, typeof(SqlXml)))
        {
            sql_md = new SqlMetaData(name, SqlDbType.Xml);
        }
        else
        {
            sql_md = default;
        }

        return sql_md;
    }
    private static int RunningThreads(List<Return_DT> All_Result)
    {

        int iRunningCount = 0;
        int iStoppedCount = 0;
        int Total_Running = 0;
        foreach (Return_DT This_Result in All_Result)
        {
            iRunningCount = 0;
            iStoppedCount = 0;
            foreach (Thread oIndividualThread in This_Result.Executions)
            {
                if (oIndividualThread.IsAlive)
                {
                    iRunningCount = iRunningCount + 1;
                    Total_Running = Total_Running + 1;
                }
                else
                {
                    iStoppedCount = iStoppedCount + 1;
                }
            }
            This_Result.Runnings = iRunningCount;
            This_Result.Stops = iStoppedCount;
        }
        return Total_Running;
    }
    private static void RefreshThreads(Return_DT This_Result)
    {
        int iRunningCount = 0;
        int iStoppedCount = 0;
        foreach (Thread oIndividualThread in This_Result.Executions)
        {
            if (oIndividualThread.IsAlive)
            {
                iRunningCount = iRunningCount + 1;
            }
            else
            {
                iStoppedCount = iStoppedCount + 1;
            }
        }
        This_Result.Runnings = iRunningCount;
        This_Result.Stops = iStoppedCount;
    }

}
internal partial class ExecuteCMD
{
    const string MSSQL = "MSSQL";
    const string MySQL = "MySQL";
    const string PGSQL = "PostgreSQL";
    const string success = "success";
    const string failed = "failed";
    private string sTemp_Folder = "";
    private remote_execution sRemote_Execution;
    public ExecuteCMD(string Temp_Folder, remote_execution This_execution)
    {
        sRemote_Execution = This_execution;
        sTemp_Folder = Temp_Folder;
    }
    public void Process()
    {
        string sCMD = "";
        string sArguments = "";
        string sInput_File = sTemp_Folder + "sql_cmd_" + DateTime.Now.ToString("yyyy_MM_dd") + "_" + sRemote_Execution.Run_ID.ToString().Replace("-", "") + ".sql";
        string sError_File = sTemp_Folder + "sql_cmd_" + DateTime.Now.ToString("yyyy_MM_dd") + "_" + sRemote_Execution.Run_ID.ToString().Replace("-", "") + ".log";
        string sOutput_File = sTemp_Folder + "sql_cmd_" + DateTime.Now.ToString("yyyy_MM_dd") + "_" + sRemote_Execution.Run_ID.ToString().Replace("-", "") + ".out";
        try
        {
            sRemote_Execution.Remote_Error = "";
            sRemote_Execution.Remote_Output = "";
            if (sRemote_Execution.CMD_Type == "SQL_CMD")
            {
                if (sRemote_Execution.DBProvider == MSSQL)
                {
                    sCMD = "SQLCMD.EXE";
                    sArguments = "-b -r1 -x -S" + sRemote_Execution.IP_or_DNS + "," + sRemote_Execution.Port + " -d" + sRemote_Execution.DBname;
                    sArguments = sArguments + " -t" + sRemote_Execution.TimeOut + " -i" + @sInput_File;
                    if (string.IsNullOrEmpty(sRemote_Execution.PWD))
                    {
                        sArguments = sArguments + " -E";
                    }
                    else
                    {
                        sArguments = sArguments + " -U" + sRemote_Execution.UserName + " -P\"" + sRemote_Execution.PWD + "\"";
                    }


                }
                if (sRemote_Execution.DBProvider == MySQL)
                {
                    sCMD = "MYSQL.EXE";
                    sArguments = "-B -h " + sRemote_Execution.IP_or_DNS + " -P " + sRemote_Execution.Port + " -D " + sRemote_Execution.DBname;
                    sArguments = sArguments + " -u " + sRemote_Execution.UserName + " -p" + sRemote_Execution.PWD;
                }
                if (sRemote_Execution.DBProvider == PGSQL)
                {
                    sCMD = "PSQL.EXE";
                    sArguments = "-q -h " + sRemote_Execution.IP_or_DNS + " -p " + sRemote_Execution.Port + " -d " + sRemote_Execution.DBname;
                    sArguments = sArguments + " -U" + sRemote_Execution.UserName + " -w ";

                }
                sArguments = sArguments + " " + sRemote_Execution.Arguments;
                WriteFile(sInput_File, sRemote_Execution.SQL);
            }
            else if (sRemote_Execution.CMD_Type == "SQL_DUMP")
            {
                if (sRemote_Execution.DBProvider == MSSQL)
                {
                    sCMD = "BCP.EXE";
                    sArguments = " " + Format_SQL_Query(sRemote_Execution.SQL);
                    if (string.IsNullOrEmpty(sRemote_Execution.Arguments))
                    {
                        if (sRemote_Execution.Remote_Output != "")
                        {
                            sArguments = sArguments + " queryout " + sRemote_Execution.Remote_Output + " -c ";
                        }
                        else
                        {
                            sArguments = sArguments + " queryout " + sOutput_File + " -c ";
                        }
                    }
                    else
                    {
                        sArguments = sArguments + sRemote_Execution.Arguments;
                    }
                    sArguments = sArguments + " -S " + sRemote_Execution.IP_or_DNS + "," + sRemote_Execution.Port + " -d " + sRemote_Execution.DBname + " -e " + sError_File;
                    if (string.IsNullOrEmpty(sRemote_Execution.PWD))
                    {
                        sArguments = sArguments + " -T";
                    }
                    else
                    {
                        sArguments = sArguments + " -U " + sRemote_Execution.UserName + " -P'" + sRemote_Execution.PWD + "'";
                    }


                }
                if (sRemote_Execution.DBProvider == MySQL)
                {
                    sCMD = "MYSQLDUMP.EXE";
                    sArguments = "-h " + sRemote_Execution.IP_or_DNS + " -P " + sRemote_Execution.Port + " -D " + sRemote_Execution.DBname;
                    sArguments = sArguments + " -u" + sRemote_Execution.UserName + " -p'" + sRemote_Execution.PWD + "'";
                    if (sRemote_Execution.Remote_Output != "")
                    {
                        sArguments = sArguments + " --result-file= " + sRemote_Execution.Remote_Output;
                    }
                    else
                    {
                        sArguments = sArguments + " --result-file= " + sInput_File;
                    }
                    sArguments = sArguments + " " + sRemote_Execution.Arguments;
                }
                if (sRemote_Execution.DBProvider == PGSQL)
                {
                    sCMD = "PG_DUMP.EXE";
                    sArguments = " -h " + sRemote_Execution.IP_or_DNS + " -p " + sRemote_Execution.Port + " -d " + sRemote_Execution.DBname;
                    sArguments = sArguments + " -U" + sRemote_Execution.UserName + " -w ";
                    sArguments = sArguments + " -f " + sInput_File;
                    if (sRemote_Execution.Remote_Output != "")
                    {
                        sArguments = sArguments + " -f " + sRemote_Execution.Remote_Output;
                    }
                    else
                    {
                        sArguments = sArguments + " -f " + sInput_File;
                    }
                }
                sArguments = sArguments + " " + sRemote_Execution.Arguments;
            }
            else
            {
                sCMD = sRemote_Execution.SQL;
                sArguments = sRemote_Execution.Arguments;
            }

            ProcessStartInfo processInfo = new ProcessStartInfo
            {
                FileName = sCMD,
                Arguments = sArguments,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using (Process process = new Process())
            {
                process.StartInfo = processInfo;
                process.Start();

                if (sRemote_Execution.DBProvider != MSSQL && sRemote_Execution.CMD_Type != "OS_CMD")
                {
                    string sqlContent = File.ReadAllText(sInput_File);
                    process.StandardInput.Write(sqlContent);
                    process.StandardInput.Close();
                }

                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                process.WaitForExit();

                if (process.ExitCode == 0)
                {
                    sRemote_Execution.Remote_Error = success;
                    if (string.IsNullOrEmpty(sRemote_Execution.Remote_Output))
                    {
                        if (File.Exists(@sOutput_File))
                        { sRemote_Execution.Remote_Output = File.ReadAllText(@sOutput_File); }
                        else
                        { sRemote_Execution.Remote_Output = output; }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(error))
                    {
                        if (File.Exists(@sError_File))
                        {
                            sRemote_Execution.Remote_Error = (sRemote_Execution.Remote_Error ?? "") + File.ReadAllText(@sError_File);
                            File.Delete(@sError_File);
                        }
                    }
                    else
                    {
                        sRemote_Execution.Remote_Error = error;
                    }
                    if (string.IsNullOrEmpty(sRemote_Execution.Remote_Error))
                    {
                        sRemote_Execution.Remote_Error = output;
                    }
                }
                if (File.Exists(@sInput_File))
                {
                    File.Delete(@sInput_File);
                }

            }
        }
        catch (Exception ex)
        {
            sRemote_Execution.Remote_Error = ex.Message;
        }
        if (string.IsNullOrEmpty(sRemote_Execution.Remote_Error))
        { sRemote_Execution.Remote_Error = success; }
    }
    private static string Format_SQL_Query(string The_SQL)
    {
        return $"\"{The_SQL.Replace("\n", "").Replace("\r", "").Replace("\"", "\\\"")}\"";
    }
    private static int WriteFile(string filename, SqlString thedata)
    {
        string errMsg = "";
        try
        {
            // Validate inputs
            if (string.IsNullOrEmpty(filename))
            {
                thedata = "Error: Filename cannot be null or empty.";
                SqlContext.Pipe.Send(thedata.ToString());
                return -100;
            }
            if (thedata.IsNull)
            {
                thedata = "Error: Input data cannot be null.";
                SqlContext.Pipe.Send(thedata.ToString());
                return -200;
            }

            // Write to file
            File.WriteAllText(filename, thedata.ToString());
            return 0; // Success
        }
        catch (UnauthorizedAccessException ex)
        {
            errMsg = $"Permission denied accessing file: {ex.Message}";
            SqlContext.Pipe.Send(errMsg);
            return -101; // Permission error
        }
        catch (IOException ex)
        {
            errMsg = $"IO error writing to file: {ex.Message}";
            thedata = errMsg;
            SqlContext.Pipe.Send(errMsg);
            return -201; // IO error
        }
        catch (Exception ex)
        {
            errMsg = $"Unexpected error: {ex.Message}";
            thedata = errMsg;
            SqlContext.Pipe.Send(errMsg);
            return -202; // General error
        }
    }
    private static string EscapeArgument(string arg)
    {
        if (string.IsNullOrEmpty(arg)) return "\"\"";
        return $"\"{arg.Replace("\"", "\\\"")}\"";
    }
}
internal partial class ExecuteSQL
{
    const string MSSQL = "MSSQL";
    const string MySQL = "MySQL";
    const string PGSQL = "PostgreSQL";

    const string success = "success";
    const string failed = "failed";
    private ConcurrentBag<DataTable> oExecuteErrors;

    private remote_execution sRemote_Execution;
    private string sExecuteDB;
    private string sExecuteDB_DBProvider;
    private string sExecuteTSQL;

    private Int32 Thetimeout;
    private ConcurrentBag<DataTable> dReaders;

    public ExecuteSQL(remote_execution This_execution, ConcurrentBag<DataTable> oReaders)
    {
        sRemote_Execution = This_execution;
        sExecuteDB = This_execution.DBname;
        sExecuteDB_DBProvider = This_execution.DBProvider;
        sExecuteTSQL = This_execution.SQL;
        dReaders = oReaders;
        Thetimeout = This_execution.TimeOut;
    }

    public void Process()
    {

        sExecuteTSQL = sRemote_Execution.SQL;
        sRemote_Execution.AffectedRows = 0;
        sRemote_Execution.Remote_Error = "";
        sExecuteDB_DBProvider = sRemote_Execution.DBProvider;
        DataTable error_dt = new DataTable();
        DataTable dt = new DataTable();
        if (sExecuteDB_DBProvider == PGSQL)
        {

            string connectionString = "Driver={" + sRemote_Execution.Driver + "};" +
                                     "Server=" + sRemote_Execution.IP_or_DNS + ";" +
                                     "Database=" + sExecuteDB + ";" +
                                     "Uid=" + sRemote_Execution.UserName + ";" +
                                     "Port=" + sRemote_Execution.Port.ToString() + ";" +
                                     "Pwd=" + sRemote_Execution.PWD + ";" + sRemote_Execution.Connection_Options;

            using (OdbcConnection oConn = new OdbcConnection(connectionString))
            {
                try
                {
                    oConn.Open();

                    using (OdbcCommand oCmd = new OdbcCommand(sExecuteTSQL, oConn))
                    {
                        oCmd.CommandTimeout = Thetimeout;
                        OdbcDataReader reader = oCmd.ExecuteReader();

                        while (!reader.IsClosed)
                        {
                            DataTable tmp_dt = new DataTable();
                            tmp_dt.Load(reader);
                            if (!(tmp_dt is null))
                            {
                                if (tmp_dt.Columns.Contains("remote_error"))
                                {
                                    foreach (DataRow dr in tmp_dt.Rows)
                                    {
                                        sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + dr["remote_error"] ?? "Null" + Environment.NewLine;
                                    }
                                }
                                else
                                {
                                    dt.Merge(tmp_dt, true);
                                    sRemote_Execution.AffectedRows = sRemote_Execution.AffectedRows + tmp_dt.Rows.Count;
                                }
                            }
                            else
                            {
                                string Errmsg = "tmp_dt load null-";
                                sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
                            }
                        }
                    }
                }
                catch (OdbcException e)
                {
                    string Errmsg = "OdbcException:";

                    for (int i = 0; i < e.Errors.Count; i++)
                    {
                        Errmsg += "Index #" + i + "\n" +
                                         "Message: " + e.Errors[i].Message + "\n" +
                                         "NativeError: " + e.Errors[i].NativeError.ToString() + "\n" +
                                         "Source: " + e.Errors[i].Source + "\n" +
                                         "SQL: " + e.Errors[i].SQLState + "\n";
                    }

                    sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
                }
                catch (Exception ex)
                {
                    string Errmsg = $"ODBC Error: {ex.Message}";
                    sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
                }
                finally
                {
                    if (dt.Columns.Count > 0) { Add_Run_ID(dt); dReaders.Add(dt); }
                    oConn.Close();
                    oConn.Dispose();
                }
            }

        }
        if (sExecuteDB_DBProvider == MySQL)
        {

            string connectionString = "Driver={" + sRemote_Execution.Driver + "};" +
                                     "Server=" + sRemote_Execution.IP_or_DNS + ";" +
                                     "Database=" + sExecuteDB + ";" +
                                     "Uid=" + sRemote_Execution.UserName + ";" +
                                     "Port=" + sRemote_Execution.Port.ToString() + ";" +
                                     "Pwd={" + (sRemote_Execution.PWD).Replace("{", "{{") + "};" + sRemote_Execution.Connection_Options;


            using (OdbcConnection oConn = new OdbcConnection(connectionString))
            {
                try
                {
                    oConn.Open();

                    using (OdbcCommand oCmd = new OdbcCommand(sExecuteTSQL, oConn))
                    {
                        oCmd.CommandTimeout = Thetimeout;
                        OdbcDataReader reader = oCmd.ExecuteReader();

                        while (!reader.IsClosed)
                        {
                            DataTable tmp_dt = new DataTable();
                            tmp_dt.Load(reader);
                            if (!(tmp_dt is null))
                            {
                                if (tmp_dt.Columns.Contains("remote_error"))
                                {
                                    foreach (DataRow dr in tmp_dt.Rows)
                                    {
                                        sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + dr["remote_error"] ?? "Null" + Environment.NewLine;
                                    }
                                }
                                else
                                {
                                    dt.Merge(tmp_dt, true);
                                    sRemote_Execution.AffectedRows = sRemote_Execution.AffectedRows + tmp_dt.Rows.Count;
                                }
                            }
                            else
                            {
                                string Errmsg = "tmp_dt load null-";
                                sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
                            }
                        }

                    }
                }
                catch (OdbcException e)
                {
                    string Errmsg = "OdbcException:";

                    for (int i = 0; i < e.Errors.Count; i++)
                    {
                        Errmsg += "Index #" + i + "\n" +
                                         "Message: " + e.Errors[i].Message + "\n" +
                                         "NativeError: " + e.Errors[i].NativeError.ToString() + "\n" +
                                         "Source: " + e.Errors[i].Source + "\n" +
                                         "SQL: " + e.Errors[i].SQLState + "\n";
                    }
                    sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;

                }
                catch (Exception ex)
                {
                    string Errmsg = $"ODBC Error: {ex.Message}";
                    sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
                }
                finally
                {
                    if (dt.Columns.Count > 0) { Add_Run_ID(dt); dReaders.Add(dt); }
                    oConn.Close();
                    oConn.Dispose();
                }
            }

        }
        if (sExecuteDB_DBProvider == MSSQL)
        {
            try
            {
                using (var scope = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
                {
                    var oConn = new SqlConnection();
                    if (string.IsNullOrEmpty(sRemote_Execution.PWD))
                    {
                        oConn = new SqlConnection("Data Source=" + sRemote_Execution.IP_or_DNS + "," + sRemote_Execution.Port + ";Initial Catalog=" + sRemote_Execution.DBname + ";Integrated Security=SSPI;Network Library=DBMSSOCN;" + sRemote_Execution.Connection_Options);
                    }
                    else
                    {
                        oConn = new SqlConnection("Data Source=" + sRemote_Execution.IP_or_DNS + "," + sRemote_Execution.Port + ";Initial Catalog=" + sRemote_Execution.DBname + ";Network Library=DBMSSOCN;user id=" + sRemote_Execution.UserName + ";password='" + sRemote_Execution.PWD + "';" + sRemote_Execution.Connection_Options);
                    }

                    try
                    {
                        oConn.Open();
                        bool checkreturn = false;
                        SqlParameter returnValueParam = null;
                        SqlCommand oCmd = oConn.CreateCommand();
                        oCmd.CommandText = sExecuteTSQL;
                        oCmd.CommandTimeout = Thetimeout;
                        if (sExecuteTSQL.StartsWith("exec @return_value="))
                        {
                            checkreturn = true;
                            returnValueParam = oCmd.Parameters.Add("@return_value", SqlDbType.VarChar, 200);
                            returnValueParam.Direction = ParameterDirection.Output;
                        }
                        SqlDataReader reader = oCmd.ExecuteReader();
                        while (!reader.IsClosed)
                        {
                            DataTable tmp_dt = new DataTable();
                            tmp_dt.Load(reader);
                            if (!(tmp_dt is null))
                            {
                                if (tmp_dt.Columns.Contains("remote_json_error"))
                                {
                                    foreach (DataRow dr in tmp_dt.Rows)
                                    {
                                        sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + dr["remote_json_error"] ?? "Null" + Environment.NewLine;
                                    }
                                }
                                else
                                {
                                    dt.Merge(tmp_dt, true);
                                    sRemote_Execution.AffectedRows = sRemote_Execution.AffectedRows + tmp_dt.Rows.Count;
                                }
                            }
                            else
                            {
                                string Errmsg = "tmp_dt load null-";
                                sRemote_Execution.Remote_Error = Errmsg + Environment.NewLine;
                            }
                        }
                        if (checkreturn)
                        {
                            if (!(returnValueParam.Value is DBNull))
                            {
                                if ((string)returnValueParam.Value != "0")
                                {
                                    string Errmsg = "Return_value is " + (string)returnValueParam.Value;
                                    sRemote_Execution.Remote_Error = Errmsg + Environment.NewLine;
                                }
                            }
                        }

                    }
                    catch (SqlException ex)
                    {
                        string Errmsg = "";
                        if (ex.Number == -2)
                        {
                            Errmsg = "SqlException-Error:timeout ";
                        }
                        else
                        {
                            Errmsg = "SqlException-" + ex.Message;
                        }
                        sRemote_Execution.Remote_Error = Errmsg + Environment.NewLine;

                    }
                    catch (InvalidOperationException ex)
                    {
                        string Errmsg = "InvalidOperationException-" + ex.Message;
                        sRemote_Execution.Remote_Error = Errmsg + Environment.NewLine;

                    }
                    catch (Exception ex)
                    {
                        string Errmsg = "Exception-" + ex.Message;
                        sRemote_Execution.Remote_Error = Errmsg + Environment.NewLine;
                    }
                    finally
                    {
                        if (dt.Columns.Count > 0) { Add_Run_ID(dt); dReaders.Add(dt); }
                        oConn.Close();
                        oConn.Dispose();
                    }
                }
            }
            catch (System.Transactions.TransactionAbortedException ex)
            {
                string Errmsg = "TransactionAbortedException:" + ex.Message;
                sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
            }
            catch (ApplicationException ex)
            {
                string Errmsg = "ApplicationException:" + ex.Message;
                sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;

            }
            catch (Exception ex)
            {
                string Errmsg = "Exception:" + ex.Message;
                sRemote_Execution.Remote_Error = sRemote_Execution.Remote_Error + Errmsg + Environment.NewLine;
            }
        }
        if (string.IsNullOrEmpty(sRemote_Execution.Remote_Error))
        { sRemote_Execution.Remote_Error = success; }
    }
    private void Add_Run_ID(DataTable this_dt)
    {
        DataColumn column;
        if (!this_dt.Columns.Contains("Run_ID"))
        {
            column = new DataColumn("Run_ID");
            column.DataType = Type.GetType("System.Guid");
            column.AllowDBNull = true;
            column.DefaultValue = sRemote_Execution.Run_ID;
            column.Unique = false;
            this_dt.Columns.Add(column);
            column.SetOrdinal(0);
        }
    }
}
internal class Return_DT
{
    private Int32 vThreads;
    private Int32 vRunnings;
    private Int32 vStops;
    private Int32 vWaitings;
    private string vResult_Name;
    private ConcurrentBag<DataTable> vResult_DT;
    private List<Thread> vExecutions;
    public List<Thread> Executions
    {
        get
        {
            return vExecutions;
        }

        set
        {
            vExecutions = value;
        }
    }

    public Int32 Waitings
    {
        get
        {
            return vWaitings;
        }

        set
        {
            vWaitings = value;
        }
    }

    public Int32 Stops
    {
        get
        {
            return vStops;
        }

        set
        {
            vStops = value;
        }
    }

    public Int32 Runnings
    {
        get
        {
            return vRunnings;
        }

        set
        {
            vRunnings = value;
        }
    }

    public Int32 Threads
    {
        get
        {
            return vThreads;
        }

        set
        {
            vThreads = value;
        }
    }

    public ConcurrentBag<DataTable> Result_DT
    {
        get
        {
            return vResult_DT;
        }

        set
        {
            vResult_DT = value;
        }
    }
    public string Result_Name
    {
        get
        {
            return vResult_Name;
        }

        set
        {
            vResult_Name = value;
        }
    }

}
internal partial class remote_execution
{
    private Guid vRun_ID;
    private Int32 vThreads;
    private Int32 vTimeOut;
    private Int32 vAffectedRows;
    private string vServername;
    private string vUsername;
    private string vDBname;
    private string vDBProvider;
    private string vDriver;
    private string vPwd;
    private string vStatus;
    private string vCMD_Type;

    private string vArguments;
    private string vIP_or_DNS;
    private string vConnection_Options;
    private string vSQL;
    private string vRemote_Error;
    private string vRemote_Output;
    private string vPort;
    private string vResturn_TMP_Table;
    private bool vdebug = false;
    public string Remote_Output
    {
        get
        {
            return vRemote_Output;
        }

        set
        {
            vRemote_Output = value;
        }
    }

    public string Arguments
    {
        get
        {
            return vArguments;
        }

        set
        {
            vArguments = value;
        }
    }
    public string CMD_Type
    {
        get
        {
            return vCMD_Type;
        }

        set
        {
            vCMD_Type = value;
        }
    }
    public Int32 AffectedRows
    {
        get
        {
            return vAffectedRows;
        }

        set
        {
            vAffectedRows = value;
        }
    }

    public Int32 Threads
    {
        get
        {
            return vThreads;
        }

        set
        {
            vThreads = value;
        }
    }
    public Int32 TimeOut
    {
        get
        {
            return vTimeOut;
        }

        set
        {
            vTimeOut = value;
        }
    }
    public Guid Run_ID
    {
        get
        {
            return vRun_ID;
        }

        set
        {
            vRun_ID = value;
        }
    }
    public string Status
    {
        get
        {
            return vStatus;
        }

        set
        {
            vStatus = value;
        }
    }
    public string Remote_Error
    {
        get
        {
            return vRemote_Error;
        }

        set
        {
            vRemote_Error = value;
        }
    }
    public string Resturn_TMP_Table
    {
        get
        {
            return vResturn_TMP_Table;
        }

        set
        {
            vResturn_TMP_Table = value;
        }
    }
    public string IP_or_DNS
    {
        get
        {
            return vIP_or_DNS;
        }

        set
        {
            vIP_or_DNS = value;
        }
    }
    public string SQL
    {
        get
        {
            return vSQL;
        }

        set
        {
            vSQL = value;
        }
    }
    public string Driver
    {
        get
        {
            return vDriver;
        }

        set
        {
            vDriver = value;
        }
    }

    public string Servername
    {
        get
        {
            return vServername;
        }

        set
        {
            vServername = value;
        }
    }
    public string DBname
    {
        get
        {
            return vDBname;
        }

        set
        {
            vDBname = value;
        }
    }
    public string DBProvider
    {
        get
        {
            return vDBProvider;
        }

        set
        {
            vDBProvider = value;
        }
    }

    public string UserName
    {
        get
        {
            return vUsername;
        }

        set
        {
            vUsername = value;
        }
    }

    public string Port
    {
        get
        {
            return vPort;
        }

        set
        {
            vPort = value;
        }
    }

    public string Connection_Options
    {
        get
        {
            return vConnection_Options;
        }

        set
        {
            vConnection_Options = value;
        }
    }
    public string PWD
    {
        get
        {
            return vPwd;
        }

        set
        {
            vPwd = value;
        }
    }

    public bool Debug
    {
        get
        {
            return vdebug;
        }

        set
        {
            vdebug = value;
        }
    }
}
