import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;

/**
 *
 * @author vparonyan
 */
public class JobRunnerConnection {
    protected static enum ConnectionType {TERADATA, MYSQL, POSTGRESQL, MONGODB, UNKNOWN};
    
    static private void testConnectionType(ConnectionType type)  throws Exception {
        if (type != ConnectionType.TERADATA &&
            type != ConnectionType.MYSQL &&
            type != ConnectionType.MONGODB &&
            type != ConnectionType.POSTGRESQL) {
            
            throw new Exception("Unsupported connection type");
        }
    }
    
    static protected ConnectionType getConnectionType(String name)  throws Exception {
        ConnectionType type = ConnectionType.UNKNOWN;
        if (name.toLowerCase().equals(ConnectionType.TERADATA.toString().toLowerCase()))
            type = ConnectionType.TERADATA;
        else if (name.toLowerCase().equals(ConnectionType.MYSQL.toString().toLowerCase()))
            type = ConnectionType.MYSQL;
        else if (name.toLowerCase().equals(ConnectionType.POSTGRESQL.toString().toLowerCase()))
            type = ConnectionType.POSTGRESQL;
        else if (name.toLowerCase().equals(ConnectionType.MONGODB.toString().toLowerCase()))
            type = ConnectionType.MONGODB;
        //testConnectionType(type);
        return type;
    }
       
    static public void testConnection()  throws Exception {
        Properties prop = new Properties();
        prop.load(new FileInputStream(JobRunner.pathToConfig + "config.properties"));
        
        java.sql.Connection conn_t = getConnection(prop, ConnectionType.TERADATA);
        conn_t.close();
        Connection conn_s = getConnection(prop, ConnectionType.MYSQL);
        conn_s.close();
        Connection conn_p = getConnection(prop, ConnectionType.POSTGRESQL);
        conn_p.close();
        Connection conn_m = getConnection(prop, ConnectionType.MONGODB);
        conn_m.close();
    }

    static public java.sql.Connection getConnection(Properties prop, ConnectionType type)  throws Exception {
        return getConnection(prop, type, "", "", "");
    }
    
    static public java.sql.Connection getConnection(Properties prop, ConnectionType type, 
                                    String url, String user, String password)  throws Exception {
        LoggerHelper.logInfoStart(JobRunnerConnection.class.getName());
        //testConnectionType(type);
        String property_suffix = null;
        switch (type) {
            case TERADATA:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().region;
                DriverManager.registerDriver((Driver) Class.forName("com.teradata.jdbc.TeraDriver").newInstance());
                break;
            case MYSQL:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
                break;
            case MONGODB:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                DriverManager.registerDriver((Driver) Class.forName("mongodb.jdbc.MongoDriver").newInstance());
                break;
            case POSTGRESQL:
                property_suffix = type.toString().toLowerCase() + "." + JobManager.createInstance().env;
                DriverManager.registerDriver((Driver) Class.forName("org.postgresql.Driver").newInstance());
                break;
            default:
                throw new Exception("Unsupported type.");
        }
        
        if( url == null || url.isEmpty()) {
            url = prop.getProperty(property_suffix + ".url");
            url = "jdbc:" + type.toString().toLowerCase() +  "://" + url;
            
            if (type == ConnectionType.TERADATA) {
                url += "/TMODE=TERA,CHARSET=UTF8";
            }
        }
        
        if( user == null || user.isEmpty()) {
            user = prop.getProperty(property_suffix + ".user");
        }
        if( password == null || password.isEmpty()) {
            password = prop.getProperty(property_suffix + ".pwd");
        }
        LoggerHelper.logInfo(JobRunnerConnection.class.getName(), url + " : " + 
                                                            user + " : " + 
                                                            password);
        
        java.sql.Connection conn = DriverManager.getConnection(url, user, password);
        LoggerHelper.logInfoEnd(JobRunnerConnection.class.getName());
        return conn;
    }
        
}
