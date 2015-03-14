
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vparonyan
 */
public class SQLCreateTableJobObject extends SQLJobObject {
    public SQLCreateTableJobObject(List<Object> par, String name, String onError, JobObject.Asserter asserter) {
        super(par, name, onError, asserter);
    }
    
    private LinkedHashMap columns = null;
    
    @Override
    public void setProperties(HashMap queryParameters,
                              String tableName,
                              LinkedHashMap columns,
                              JobRunnerConnection.ConnectionType dbtype) {
        this.tableName = tableName;
        this.columns = columns;
        this.queryParameters = queryParameters;
        this.dbtype = dbtype;
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(SQLCreateTableJobObject.class.getName());
        if (!JobManager.createInstance().simulate) {
                    
            Statement stmt;
            Properties prop  = JobManager.createInstance().prop;

            String url = getJDBCURL(dbtype, (String)queryParameters.get(TargetDBURL));
            String user = queryParameters.get(TargetDBUser) != null ? (String)queryParameters.get(TargetDBUser) : "";
            String passwd = queryParameters.get(TargetDBPassword) != null ? (String)queryParameters.get(TargetDBPassword) : "";
            String dbName = queryParameters.get(TargetDBName) != null ? (String)queryParameters.get(TargetDBName) : 
                                                        prop.getProperty(dbtype.toString().toLowerCase() +  "." 
                                                                    + JobManager.createInstance().env + ".dbname");

            try (Connection conn = JobRunnerConnection.getConnection(prop, dbtype, url, user, passwd)) {

                String fullTableName = tableName;
                if (dbName != null && !dbName.isEmpty()) {
                    fullTableName = dbName + "." + fullTableName;
                }

                String query  = "CREATE TABLE " + fullTableName + "(";
                Iterator columnIter = columns.entrySet().iterator();
                while (columnIter.hasNext()) {
                    Map.Entry column = (Map.Entry) columnIter.next();
                    String val = (String)column.getValue();
                    if (val == null) {
                        val = "";
                    }
                    query += column.getKey() + " " + val;
                    if (columnIter.hasNext()) {
                        query += ",";
                    } else {
                     query += ")";
                    }   
                }
                if (queryParameters.get(TableIndex) != null) {
                    query += " " + (String)queryParameters.get(TableIndex);
                }

                query = Utility.substituteParameters(query, queryParameters);
                LoggerHelper.logInfo(JobObject.class.getName(), " Statement create table : \n " + query);

                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                LoggerHelper.logInfo(JobObject.class.getName(), " Statement object created. \n ");

                stmt.execute(query);
                stmt.close();    
            }
        }
        LoggerHelper.logInfoEnd(SQLCreateTableJobObject.class.getName());
    }
    
}
