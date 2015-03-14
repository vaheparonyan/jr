
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/*
        this tool will accept the following arguments
        -env <env_name>
        <env_name> ::= production | development | staging
        required
        
        -region <region_name>
        <region_name> ::= intl | na | needish
        required
        
        -start_day <start_day>
        <start_day> ::= start day in yyyy-MM-dd format 
        If the <start_day> is missing then the only jobs that does not 
        have dependency from <date> will be executed 
        
         -end_day <end_day>
        <end_day> ::= end day in yyyy-MM-dd format, require <start_date> 
        
        -jobs <xml_file>
         <xml_file>  ::= xml file containing the jobs
        default: the jobs.xml file.
 * @author vparonyan
 */
public class JobRunnerCLIParser {
    private final HashMap<String, String> commandLineArgs;
    
    private static String helpMessage= null;
 
    protected static String getHelpMessage() throws Exception {
        
        if (helpMessage == null) {
            StringBuilder msg = new StringBuilder();
            msg.append("\n\nHelp message: \n");
            msg.append("\njava -env <env_name> -region <region_name> [-start_day <start_day>] [-end_day <end_day>] [-jobs <xml_file>\n\n");
            msg.append("-env <env_name> \n");
            msg.append("<env_name> ::= production | development | staging \n");
            msg.append("required \n");
            msg.append("\n");
            msg.append("-region <region_name>\n");
            msg.append("<region_name> ::= intl | na | needish\n");
            msg.append("required\n");
            msg.append("\n");
            msg.append("-start_day <start_day>\n");
            msg.append("<start_day> ::= start day in yyyy-MM-dd format \n");
            msg.append("If the <start_day> is missing then the only jobs that does not ");
            msg.append("have dependency from <date> will be executed \n");
            msg.append("Defaults the <end_day> to 'today' \n");
            msg.append("\n");
            msg.append("-end_day <end_day>\n");
            msg.append("<end_day> ::= end day in yyyy-MM-dd format, require <start_date> \n");
            msg.append("\n");
            msg.append("-date_range <boolean>\n");
            msg.append("<boolean> ::= true | false, by default it is false \n");
            msg.append("\n");
            msg.append("-jobs <xml_file>\n");
            msg.append(" <xml_file>  ::= xml file containing the jobs\n");
            msg.append("default: the jobs.xml file.\n\n");
            msg.append("\n");
            msg.append("-path_to_config <path>\n");
            msg.append(" <path>  ::= folder containing configuration file\n");
            msg.append("default: the ../jobRunner.\n\n");
            msg.append("\n");
            msg.append("-path_to_scripts <path>\n");
            msg.append(" <path>  ::= folder containing script files\n");
            msg.append("default: the ../jobRunnerScripts.\n\n");
            msg.append("\n");
            msg.append("-simulate\n");
            msg.append("Just simulates the jobs execution, i.e. no job is executed, added just for debuginng purpoose.\n");
            msg.append("default: false.\n\n");
            
            helpMessage = msg.toString();
        }
        return helpMessage;
    }
    
    public JobRunnerCLIParser(String args[]) throws Exception {
        LoggerHelper.logInfoStart(JobRunnerCLIParser.class.getName());
        
        this.commandLineArgs = new HashMap();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-env":
                    commandLineArgs.put("env", args[++i]);
                    break;
                case "-region":
                    commandLineArgs.put("region", args[++i]);
                    break;
                case "-start_day":
                    commandLineArgs.put("start_day", args[++i]);
                    // as the start day exist, then put the end day as today
                    Date ed = new Date(Calendar.getInstance().getTime().getTime());
                    commandLineArgs.put("end_day", df.format(ed));
                    break;
                case "-end_day":
                    if (commandLineArgs.containsKey("start_day")) {
                        commandLineArgs.put("end_day", args[++i]);
                    } else {
                        throw new Exception("start_day must be specified before end_day + \n" + getHelpMessage());
                    }
                    break;
                case "-jobs":
                    commandLineArgs.put("jobs", args[++i]);
                    break;
                case "-date_range":
                    commandLineArgs.put("date_range", args[++i].toLowerCase());
                    break;
                case "-simulate":
                    commandLineArgs.put("simulate", "true");
                    break;
                case "-path_to_config":
                    commandLineArgs.put("path_to_config", args[++i]);
                    break;
                case "-path_to_scripts":
                    commandLineArgs.put("path_to_scripts", args[++i]);
                    break;
                default:
                    throw new Exception("Unknown argument: " + args[i] + "\n" + getHelpMessage());
            }
        }
        
        LoggerHelper.logInfoEnd(JobRunnerCLIParser.class.getName());
    }
    
    public String getArgument(String argName) {
        if (this.commandLineArgs.containsKey(argName)) {
            return this.commandLineArgs.get(argName);
        }
        return null;
    }
}
