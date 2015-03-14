import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author vparonyan
 */
public abstract class JobObject {
       
    private final List<Object> params;
    final String name;
    private final List<String> dependentObjects = new ArrayList();
    private Future future;
    private boolean isChild;
    private Exception exception = null;
    private final String onError;
    private int retryNum = 0;
    private int retryPeriod = 0;

    public static enum AsserterDataType {INTEGER, VARCHAR};
    public static enum AsserterCondition {EQUAL, LESSER, GREATER};
    
    public static class Asserter<VT> {
        public AsserterDataType type;
        public AsserterCondition condition;
        public int column;
        public VT value;
        
        Asserter(AsserterDataType type, AsserterCondition condition, int column, VT value) {
            this.type = type;
            this.condition = condition;
            this.column = column;
            this.value = value;
        }
    }
    
    protected final Asserter asserter;
        
    public JobObject(List<Object> par, String name, String onError, Asserter asserter){
        this.params = par;
        this.name = name;
        this.future = null;
        
        this.onError = onError.split(",")[0];
        if (onError.split(",").length > 2) {
            retryNum = Integer.parseInt(onError.split(",")[1]);
            retryPeriod = Integer.parseInt(onError.split(",")[2]);
        }
        this.asserter = asserter;
    }
    
    public String getOnErrorAction() {
        return this.onError;
    }
    
    public String getName() {
        return name;
    }
    
    public List<Object> getParameters() {
        return params;
    }
    
    public String getMethod() {
        return "execute";
    }
    
    public String getPropertiesMethod() {
        return "setProperties";
    }
            
    public Asserter getAsserter() {
        return this.asserter;
    }
    
    public boolean hasAsserter() {
        return this.asserter != null;
    }
    
    public synchronized void addDependency(String n) {
        if (n != null) {
            dependentObjects.add(n);
        }
    }
    
    public synchronized List<String> getDependentObjects() {
        return dependentObjects;
    }
    
    public synchronized boolean hasDependencies() {
        return !dependentObjects.isEmpty();
    }
    
    public synchronized void isChild(boolean isChild) {
        this.isChild = isChild;
    }
    
    public synchronized boolean isChild() {
        return isChild;
    }
    
    public synchronized void setFuture(Future f) {
        future = f;
    }
    
    public synchronized Future getFuture() {
        return future;
    }
    
    public synchronized void clearException() {
        exception = null;
    }
    
    public synchronized Exception getException() {
        return exception;
    }

    public synchronized void setException(Exception ex) {
        if (exception == null) {
            exception = ex;
        }
    }
    
    public synchronized int getRetryNum() {
        return retryNum;
    }

    public synchronized int getRetryPeriod() {
        return retryPeriod;
    }

    public abstract void execute() throws Exception;
    
    public void setProperties(HashMap queryParameters,
                              String queryFileName, 
                              JobRunnerConnection.ConnectionType dbtype,
                              JobRunnerConnection.ConnectionType targetDbType,
                              String tableName,
                              ArrayList resultSetFieldNames) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
    
    public void setProperties(HashMap queryParameters,
                              String queryFileName, 
                              JobRunnerConnection.ConnectionType dbtype) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
    
    public void setProperties(HashMap params, String command, String outputFile) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }

    public void setProperties(HashMap a, String b, HashMap c) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
    
    public void setProperties(HashMap params) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
    
    public void setProperties(HashMap queryParameters,
                          String tableName,
                          LinkedHashMap columns,
                          JobRunnerConnection.ConnectionType dbtype) throws Exception {
        throw new Exception("Internal Error: This function should not been called. The derived method should be called.");
    }
   
    public synchronized void dummyJob() throws Exception {
        LoggerHelper.logInfoStart(JobObject.class.getName());
        LoggerHelper.logInfoEnd(JobObject.class.getName());
    }

   
}

