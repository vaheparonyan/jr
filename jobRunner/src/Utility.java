import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;

public class Utility {
    protected static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
        
    public static String getQuery(String fileName) throws Exception{
        LoggerHelper.logInfoStart(Utility.class.getName());
        
        BufferedReader in;
        StringBuilder res = new StringBuilder();
        String content;

        in = new BufferedReader(new FileReader(JobRunner.pathToScripts + "sql/"+fileName+".sql"));
        String line;
        while ((line = in.readLine()) != null) res.append(line).append("\n");
        content = res.toString();
        
        in.close();
//        LoggerHelper.logInfo(Utility.class.getName(), content);
        LoggerHelper.logInfoEnd(Utility.class.getName());
        return content;
    }

    public static String substituteParameters(String str, HashMap params){
        String ret = str;
        if (params != null) {
            Iterator iter = params.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry param = (Map.Entry) iter.next();
                String val = (String)param.getValue();
                if (val == null) {
                    val = "";
                }
                if (param.getKey() != null) {
                    ret = ret.replace("@parameter_"+param.getKey(), val);
                }
            }
            iter = params.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry param = (Map.Entry) iter.next();
                String val = (String)param.getValue();
                if (val == null) {
                    val = "";
                }
                ret = ret.replace("@parameter_"+param.getKey(), val);
            }
        }
        return ret;
    }
    
    public static void sendAlert(String to, String error, String subject) throws Exception {
        LoggerHelper.logInfoStart(Utility.class.getName());
        
        ExecuteBash bash = new ExecuteBash(null, null, JobRunnerXMLParser.ReservedKeywords.FAIL.toString().toLowerCase(), null);
        String command = "echo \"" + error + "\" | mailx -s \"" + subject + "\" " + to;
        bash.setProperties(new HashMap(), command, "");
        bash.execute();

        LoggerHelper.logInfoEnd(Utility.class.getName());
    }

    public static void execScript(String comm, String alterTo) throws Exception{
        LoggerHelper.logInfoStart(Utility.class.getName());
        String line;
        Process p = Runtime.getRuntime().exec(comm);
        try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            while ((line = input.readLine()) != null)
                LoggerHelper.logInfo(Utility.class.getName(), line);
        }
        LoggerHelper.logInfoEnd(Utility.class.getName());
    }
    
    public static String generateFileName(String prefix, String postfix) {
        SecureRandom random = new SecureRandom();
        return prefix + new BigInteger(130, random).toString(32) + postfix;
    }
    
    public static int copy(InputStreamReader input, Writer output) throws IOException
    {
        char[] buffer = new char[DEFAULT_BUFFER_SIZE];
        int count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer)))
        {
            if (output != null) {
                output.write(buffer, 0, n);
            }
            count += n;
        }
        return count;
    }
}
