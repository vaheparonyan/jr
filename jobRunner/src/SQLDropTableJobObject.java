
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
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
public class SQLDropTableJobObject extends SQLJobObject {

    public SQLDropTableJobObject(List<Object> par, String name, String onError, Asserter asserter) {
        super(par, name, onError, asserter);
    }
    
    @Override
    public void setProperties(HashMap queryParameters,
                              String tableName, 
                              JobRunnerConnection.ConnectionType dbtype) {
        setProperties(queryParameters, null, dbtype, null, tableName, null);
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(SQLDropTableJobObject.class.getName());

        if (!JobManager.createInstance().simulate) {
                    
            if ( this.tableName == null || this.queryParameters == null || this.dbtype == null) {
                throw new Exception ("The properties are not set - tableName: " + this.tableName + 
                                                                   ", queryParameters : " + this.queryParameters.toString() + 
                                                                   ", dbtype : " + this.dbtype.toString() );
            }

            Statement stmt;
            Properties prop  = JobManager.createInstance().prop;

            String url = getJDBCURL(dbtype, (String)queryParameters.get(TargetDBURL));
            String user = queryParameters.get(TargetDBUser) != null ? (String)queryParameters.get(TargetDBUser) : "";
            String passwd = queryParameters.get(TargetDBPassword) != null ? (String)queryParameters.get(TargetDBPassword) : "";
            String dbName = queryParameters.get(TargetDBName) != null ? (String)queryParameters.get(TargetDBName) : 
                                                        prop.getProperty(dbtype.toString().toLowerCase() +  "." 
                                                                    + JobManager.createInstance().env + ".dbname");

            try (Connection conn = JobRunnerConnection.getConnection(prop, dbtype, url, user, passwd)) {

                String fullTableName = this.tableName;
                if (dbName != null && !"".equals(dbName)) {
                    fullTableName = dbName + "." + fullTableName;
                }

                String query  = "DROP TABLE " + fullTableName;

                query = Utility.substituteParameters(query, queryParameters);

                LoggerHelper.logInfo(JobObject.class.getName(), "Drop Table query: \n" + query);

                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

                stmt.execute(query);
                stmt.close();
            }
            
        }

        LoggerHelper.logInfoEnd(SQLDropTableJobObject.class.getName());
    }
    
}
