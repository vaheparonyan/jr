import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;


public class SQLJobObject extends JobObject{
    protected static final String TargetDBURL = "TargetDBURL";
    protected static final String TargetDBUser = "TargetDBUser";
    protected static final String TargetDBPassword = "TargetDBPassword";
    protected static final String TargetDBName = "TargetDBName";
    protected static final String SourceDBURL = "SourceDBURL";
    protected static final String SourceDBUser = "SourceDBUser";
    protected static final String SourceDBPassword = "SourceDBPassword";
    protected static final String TableIndex = "TableIndex";
    
    public SQLJobObject(List<Object> par, String name, String onError, Asserter asserter){
        super(par, name, onError, asserter);
    }
    
    private ResultSet pullData( Connection conn, 
                                String query) throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        Statement stmt;
        ResultSet rs = null;
        
        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        
        if (!JobManager.createInstance().simulate)
        {
            rs = stmt.executeQuery(query);
        }
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
        return rs;
    }
    
    private synchronized String dumpData(ResultSet rs, String tableName)  throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());

        String fileName = "/tmp/" + Utility.generateFileName(tableName, ".tsv");
        
        LoggerHelper.logInfo(SQLJobObject.class.getName(), "Dumping source table'" + 
                                tableName+ "' into '" + fileName + "' file.");
        
        Writer writer = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(fileName), "utf-8"));

        while (rs.next()) {
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                InputStream in = new ByteArrayInputStream("".getBytes("UTF-8"));
                if (rs.getBinaryStream(i+1) != null) {
                    in = rs.getBinaryStream(i+1);
                }
                Utility.copy(new InputStreamReader(in), writer);
                
                in = new ByteArrayInputStream("\t".getBytes("UTF-8"));
                Utility.copy(new InputStreamReader(in), writer);
            }
            InputStream in = new ByteArrayInputStream("\n".getBytes("UTF-8"));
            Utility.copy(new InputStreamReader(in), writer);
            
            writer.flush();
        }
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
        return fileName;
    }
    
    protected String getJDBCURL(JobRunnerConnection.ConnectionType dbtype, String url) throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        LoggerHelper.logInfo(SQLJobObject.class.getName(), 
                "dbtype = " + dbtype.toString().toLowerCase() + ", url = " + url);

        String ret = "";
        if (url != null && !url.equals("")) {
            ret = "jdbc:" + dbtype.toString().toLowerCase() + "://" + url;
            if (dbtype == JobRunnerConnection.ConnectionType.TERADATA) {
                ret += "/TMODE=TMODE,CHARSET=UTF8";
            }
        }
        LoggerHelper.logInfo(SQLJobObject.class.getName(), "returned URL = " + ret);
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
        return ret;
    }

    protected String getUrl(JobRunnerConnection.ConnectionType type, String url) throws Exception {
        Properties prop  = JobManager.createInstance().prop;
        
        String property_suffix = null;
        switch (type) {
            case TERADATA:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().region;
                break;
            case MYSQL:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                break;
            case MONGODB:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                break;
            case POSTGRESQL:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                break;
            default:
                throw new Exception("Unsupported type.");
        }
        
        if( url == null || url.isEmpty()) {
            url = prop.getProperty(property_suffix + ".url");
        }
        return url;
    }
    
    protected String queryFileName = null; 
    protected HashMap queryParameters = null;
    protected JobRunnerConnection.ConnectionType dbtype = null;
    protected JobRunnerConnection.ConnectionType targetDbType = null;
    protected String tableName = null;
    protected ArrayList resultSetFieldNames = null;
                                      
    @Override
    public void setProperties(HashMap queryParameters,
                              String queryFileName, 
                              JobRunnerConnection.ConnectionType dbtype,
                              JobRunnerConnection.ConnectionType targetDbType,
                              String tableName,
                              ArrayList resultSetFieldNames) {
        this.queryFileName = queryFileName;
        this.queryParameters = queryParameters;
        this.dbtype = dbtype;
        this.targetDbType = targetDbType;
        this.tableName = tableName;
        this.resultSetFieldNames = resultSetFieldNames;
    }
    
    @Override
    public void setProperties(HashMap queryParameters,
                              String queryFileName, 
                              JobRunnerConnection.ConnectionType dbtype) {
        setProperties(queryParameters, queryFileName, dbtype, null, null, null);
    }
    
    @Override
    public void setProperties(HashMap queryParameters,
                          String tableName,
                          LinkedHashMap columns,
                          JobRunnerConnection.ConnectionType dbtype) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        if (!JobManager.createInstance().simulate) {
                    
            if ( this.queryFileName == null || this.queryParameters == null || this.dbtype == null) {
                throw new Exception ("The properties are not set - queryFileName: " + this.queryFileName + 
                                                                   ", queryParameters : " + this.queryParameters.toString() + 
                                                                   ", dbtype : " + this.dbtype.toString() );
            }

            if (this.targetDbType == null && this.tableName == null && this.resultSetFieldNames == null) {
                executeSimpleQuery();
            } else if (this.targetDbType == null || this.tableName == null || this.resultSetFieldNames == null) {
                throw new Exception ("The properties are not set - targetDbType: " + this.targetDbType.toString() + 
                                                                   ", resultSetFieldNames : " + this.resultSetFieldNames.toString() + 
                                                                   ", tableName : " + this.tableName.toString() );
            } else {
                executeQuery();
            }
        }
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());        
    }
    
    private void executeQuery() throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());

        assert(resultSetFieldNames != null);
        
        ResultSet rs;
        
        String query;
        try {
            query = Utility.getQuery(queryFileName);
        } catch (FileNotFoundException e) {
            query = queryFileName;
        }
        query = Utility.substituteParameters(query, queryParameters);
        
        LoggerHelper.logInfo(SQLJobObject.class.getName(), "Statement object created. \n " + query);

        Properties prop  = JobManager.createInstance().prop;
        
        String sourceUrl = getJDBCURL(dbtype, (String)queryParameters.get(SourceDBURL));
        String sourceUser = queryParameters.get(SourceDBUser) != null ? (String)queryParameters.get(SourceDBUser) : "";
        String sourcePasswd = queryParameters.get(SourceDBPassword) != null ? (String)queryParameters.get(SourceDBPassword) : "";

        
         // scp temp file to the target host
        String targetUrl = getUrl(this.targetDbType, (String)queryParameters.get(TargetDBURL));
        
        // do smart load(depends on type of target)
        String targetUser = queryParameters.get(TargetDBUser) != null ? (String)queryParameters.get(TargetDBUser) : "";
        String targetPasswd = queryParameters.get(TargetDBPassword) != null ? (String)queryParameters.get(TargetDBPassword) : "";
        String targetDbName = queryParameters.get(TargetDBName) != null ? (String)queryParameters.get(TargetDBName) : 
                                                prop.getProperty(targetDbType.toString().toLowerCase() +  "." 
                                                            + JobManager.createInstance().env + ".dbname");
        
        String fileName;
        if (targetDbType != dbtype || !targetUrl.equals(sourceUrl)) {
            try (Connection conn = JobRunnerConnection.getConnection(prop, dbtype, sourceUrl, sourceUser, sourcePasswd)) {
                rs = pullData(conn, query);
                
                // select data into temp file
                fileName = dumpData(rs, tableName);
            }
            
            if (fileName == null) {
                throw new Exception("Internal Error. The filename is null .... ");
            }
            
            if (targetDbType == JobRunnerConnection.ConnectionType.MYSQL) {
                handleSqlQuery(tableName, resultSetFieldNames, targetUrl, targetUser, targetPasswd, targetDbName, fileName);
            } else if (targetDbType == JobRunnerConnection.ConnectionType.TERADATA) {
                handleTeradataQuery(tableName, resultSetFieldNames, targetUrl, targetUser, targetPasswd, targetDbName, fileName);
            } else {
                throw new Exception("not implemented yet.");
            }
        } else {
            targetUrl = getJDBCURL(this.targetDbType, (String)queryParameters.get(TargetDBURL));
            handleSimpleQuery(targetDbType, tableName, resultSetFieldNames, targetUrl, targetUser, targetPasswd, targetDbName, query);
        }
        
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
    }

    private void executeSingleQuery(Connection conn, String query) throws SQLException, Exception {
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        
        LoggerHelper.logInfo(SQLJobObject.class.getName(), " Statement object created :  " + query + "\n");
        if (!JobManager.createInstance().simulate)
        {
            try {
                stmt.execute(query);
            } catch (SQLException e) {
                throw new Exception(query + " : " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
            }

            if (this.asserter != null) {

                int index = this.asserter.column;
                JobObject.AsserterCondition c = this.asserter.condition;
                Object v1 = this.asserter.value;
                if (this.asserter.type == JobObject.AsserterDataType.INTEGER)  {
                    v1 = Integer.parseInt(this.asserter.value.toString());
                }

                if (!stmt.getResultSet().next()) {
                    stmt.close();
                    throw new Exception("Expected to get results for asserter.");
                }

                if (stmt.getResultSet().getMetaData().getColumnCount() < index) {
                    stmt.close();
                    throw new Exception("The column number is not correct : index = " + index + 
                                        ", size = " + stmt.getResultSet().getMetaData().getColumnCount());
                }

                if (!stmt.getResultSet().getMetaData().getColumnTypeName(index).toUpperCase().equals(this.asserter.type.toString())) {
                    stmt.close();
                    throw new Exception("The column type is not correct :" + this.asserter.type.toString());
                }
                Object v2 = stmt.getResultSet().getObject(index);
                if (this.asserter.type == JobObject.AsserterDataType.INTEGER)  {
                    v2 = Integer.parseInt(v2.toString());
                }
                //negative - less, 0 - equal, positive - greater
                int diff = ((Comparable)(v2)).compareTo(v1);

                if ( (c == JobObject.AsserterCondition.EQUAL && diff != 0 ) ||
                     (c == JobObject.AsserterCondition.GREATER && diff <= 0) ||
                     (c == JobObject.AsserterCondition.LESSER && diff >= 0)) {

                    stmt.close();
                    throw new Exception("Assertion(" + c.toString() + ") failed on : " + v1.toString() + " - " + v2.toString());
                }
            }
            stmt.close();
        }
    }
    
    private void executeSimpleQuery() throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());

        Properties prop  = JobManager.createInstance().prop;
        
        String url = getJDBCURL(dbtype, (String)queryParameters.get(TargetDBURL));
        String user = queryParameters.get(TargetDBUser) != null ? (String)queryParameters.get(TargetDBUser) : "";
        String passwd = queryParameters.get(TargetDBPassword) != null ? (String)queryParameters.get(TargetDBPassword) : "";

        try (Connection conn = JobRunnerConnection.getConnection(prop, dbtype, url, user, passwd)) {
        
            List<String> queries;

            try {
                queries = Arrays.asList(Utility.getQuery(queryFileName).split("SQL_BREAK;"));
            } catch (FileNotFoundException e) {
                queryFileName = Utility.substituteParameters(queryFileName, queryParameters);
                queries = Arrays.asList(queryFileName.split("SQL_BREAK;"));
            }

            for (String query : queries) {
                query = Utility.substituteParameters(query, queryParameters);
                
                executeSingleQuery(conn, query);

            }
        }
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
    }


    private JobObject cleanTdloadTables(JobObject drop, String name) throws Exception {
        if (drop == null) {
            drop = new SQLDropTableJobObject(null, null, JobRunnerXMLParser.ReservedKeywords.IGNORE.toString().toLowerCase(), null);
        }
       
        //cleanup, using filename as temp table name
        drop.setProperties(new HashMap(), name+"_UV", JobRunnerConnection.ConnectionType.TERADATA);
        drop.execute();
        drop.setProperties(new HashMap(), name+"_ET", JobRunnerConnection.ConnectionType.TERADATA);
        drop.execute();
        drop.setProperties(new HashMap(), name+"_log", JobRunnerConnection.ConnectionType.TERADATA);
        drop.execute();
        drop.setProperties(new HashMap(), name, JobRunnerConnection.ConnectionType.TERADATA);
        drop.execute();
        
        return drop;
    }
    
    private void handleTeradataQuery(String tableName, ArrayList resultSetFieldNames, 
                                     String url, String user, String passwd, 
                                     String dbName, String fileName) throws Exception {
        
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        
        JobObject bash = new ExecuteBash(null, null, 
                JobRunnerXMLParser.ReservedKeywords.FAIL.toString().toLowerCase(), null);
        JobObject drop = cleanTdloadTables(null, fileName);
        
        //create temp table to load data into
        String commandLine = "tdsql -H " + url + " -u " + user + " -p " + passwd + 
                " \"CREATE MULTISET TABLE " + dbName + "." + fileName +
                ",NO FALLBACK ,NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT" +
                ", DEFAULT MERGEBLOCKRATIO AS (select ";

        for (Object n : resultSetFieldNames) {
            commandLine += (String)n + ",";
        } 
        commandLine = commandLine.replaceAll(",$", " ");
        commandLine += "FROM " + dbName + "." + tableName + ") WITH NO DATA\";";

        // load data into temp table and insert into main from temp
        commandLine += "tdload -h " + url + " -u " + user + " -p " + passwd + " -f " + 
                fileName + " -d TAB -t " + fileName + " --TargetWorkingDatabase " + dbName + ";";

        commandLine += "rm -rf " + fileName + ";";

        commandLine += "tdsql -H " + url + " -u " + user + " -p " + passwd + 
                        " \"insert into " + dbName + "." + tableName + "(";

        for (Object n : resultSetFieldNames) {
            commandLine += (String)n + ",";
        }
        commandLine = commandLine.replaceAll(",$", ")");

        commandLine += "select * from " + dbName + "." + fileName + "\"";

        bash.setProperties(new HashMap(), commandLine, "");
        bash.execute();

        cleanTdloadTables(drop, fileName);
         
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
    }

     private void handleSimpleQuery(JobRunnerConnection.ConnectionType dbtype,
                                    String tableName, ArrayList resultSetFieldNames, 
                                     String url, String user, String passwd, 
                                     String dbName, String query) throws Exception {
        
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        Properties prop  = JobManager.createInstance().prop;
        try (Connection conn = JobRunnerConnection.getConnection(prop, dbtype, url, user, passwd)) {
            String fullTableName = tableName;
            if (dbName != null && !dbName.isEmpty()) {
                fullTableName = dbName + "." + fullTableName;
            }
            
            // insert into <tblname>(column list ... ) + actual query    
            String insertQuery  = "INSERT INTO " + fullTableName + "(";
            for (Object n : resultSetFieldNames) {
                insertQuery += (String)n + ",";
            }
            insertQuery = insertQuery.replaceAll(",$", ") ");
            insertQuery += query;
            
            LoggerHelper.logInfo(JobObject.class.getName(), "Insert query:" + insertQuery);
            
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            if (!JobManager.createInstance().simulate)
            {
                stmt.execute(insertQuery);
                stmt.close();
            }
        }
        
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
    }

     
    private void handleSqlQuery(String tableName, ArrayList resultSetFieldNames,
                                String url, String user, String passwd, 
                                String dbName, String fileName) throws Exception {
        LoggerHelper.logInfoStart(SQLJobObject.class.getName());
        
        ExecuteBash bash = new ExecuteBash(null, null, 
                    JobRunnerXMLParser.ReservedKeywords.FAIL.toString().toLowerCase(), null);
       
        String commandLine = "PATH=/usr/local/bin/:/usr/bin/:$PATH && mysql -h " + url + " -u " + user + " -p" + passwd + " -D " + dbName + 
                " -e \"load data local infile \\\"" + fileName + "\\\" into table " + tableName + "(";
        
        for (Object n : resultSetFieldNames) {
            commandLine += (String)n + ",";
        }
        commandLine = commandLine.replaceAll(",$", "");
        commandLine += ")\"";

        bash.setProperties(new HashMap(), commandLine, "");
        bash.execute();

        // remove the temporary files.
        bash.setProperties(new HashMap(), "rm -rf " + fileName, "");
        bash.execute();
        
        LoggerHelper.logInfoEnd(SQLJobObject.class.getName());
    }

}
