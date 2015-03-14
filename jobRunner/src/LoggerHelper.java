import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.apache.log4j.Logger;

/**
 *
 * @author vparonyan
 */
public class LoggerHelper {
    
    private static final Map<String, Logger> loggers = new HashMap();
    private static final Stack<Long> loggingTimes = new Stack();
    private static boolean isDebug = true;

    private static String getMethodName()
    {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();

        return ste[3].getMethodName(); //Thank you Tom Tresansky
    }
    
    public static void setDebug(boolean d) {
        isDebug = d;
    }
            
    public static synchronized void logInfoStart(String name) throws Exception{
        if (!isDebug) return;
        if (loggers.get(name) == null) {
            loggers.put(name, Logger.getLogger(name));
        }
        
        loggingTimes.push(Calendar.getInstance().getTimeInMillis());   
        loggers.get(name).info("<< Starting " + 
                               getMethodName() +  
                               " in " + Thread.currentThread().getName() + "... ");
    }    
    
    public static synchronized void logInfoEnd(String name) throws Exception{
        if (!isDebug) return;
        if (loggers.get(name) == null ||
            loggingTimes.isEmpty()) {
            throw new Exception("Couldnot get the logger object.");
        }
        long duration = Calendar.getInstance().getTimeInMillis() - loggingTimes.pop();
        long miliseconds = duration%1000;
        long seconds = duration/1000%60;
        long minutes = duration/1000/60%60;
        long hours = duration/1000/3600;


        loggers.get(name).info(">> End of " + 
                               getMethodName() + " in " + Thread.currentThread().getName() + 
                               " : tooks : " + 
                               String.format("%02d", hours) + ":" +
                               String.format("%02d", minutes) + ":" + 
                               String.format("%02d", seconds) + "." +
                               String.format("%03d", miliseconds));
    }
    
    public static synchronized void logInfoStart(String name, boolean silent) throws Exception{
        if (!isDebug || silent) return;
        logInfoStart(name);
    }    
    
    public static synchronized void logInfoEnd(String name, boolean silent) throws Exception{
        if (!isDebug || silent) return;
        logInfoEnd(name);
    }
    
    public static synchronized void logError(String name, String content) throws Exception{
        if (loggers.get(name) == null) {
            loggers.put(name, Logger.getLogger(name));
        }
        
        loggers.get(name).error(" in " + Thread.currentThread().getName() + ", " + content);
    }
    
    public static synchronized void logInfo(String name, String content) throws Exception{
        if (loggers.get(name) == null) {
            loggers.put(name, Logger.getLogger(name));
        }
        
        loggers.get(name).info(" in " + Thread.currentThread().getName() + ", " + content);
    }
}
