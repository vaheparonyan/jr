import java.io.FileInputStream;
import java.util.*;
import java.text.*;
import java.util.concurrent.*;
        
public class JobRunner {
    
    static final String criticalMsg = "CRITICAL in <project>: ";
    static final String startingMsg = "STARTING the <project> updates: ";
    static final String successMsg = "SUCCESSFULLY FINISHED the <projecy>  updates: ";
    
    //this one should be in configuration
    static public String pathToScripts = "";
    static public String pathToConfig = "";

    public static void updateJobRunner(String env, 
                                   Date date, 
                                   Date end_day,
                                   Boolean dt_range,
                                   String region, 
                                   String xmlFileName, 
                                   boolean simulate) throws Exception {
        LoggerHelper.logInfoStart(JobRunner.class.getName());
                        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        JobManager jobs = JobManager.createInstance();
        //JobRunnerConnection.testConnection();
        String st_dt = date != null ? df.format(date) : null;
        String end_dt = end_day != null ? df.format(end_day) : null;
        jobs.setParams(env, st_dt, end_dt, region, simulate);
        
        if (dt_range || (date == null || end_day == null)) {
            jobs.execute(pathToScripts + "xml/" + xmlFileName);
        } else {
            while (!df.format(date).equals(end_dt)) {
                jobs.setParams(env, df.format(date), end_dt, region, simulate);
                jobs.execute(pathToScripts + "xml/" + xmlFileName);

                date = new Date(date.getTime() + TimeUnit.DAYS.toMillis(1));
            }
        }

        LoggerHelper.logInfoEnd(JobRunner.class.getName());
    }

    public static void main(String args[]) throws Exception {
        LoggerHelper.setDebug(true);
        LoggerHelper.logInfoStart(JobRunner.class.getName());
        String env = null, region = null, st_day = "", en_day = "", jobs;
        Date start_day = null, end_day = null;
        Boolean dt_range, simulate;

        String subject = "Undefined", body = "Undefined";
        JobRunnerLog.JobRunnerStatus status = JobRunnerLog.JobRunnerStatus.UNKNOWN;
        
        Properties prop = new Properties();
        
        JobRunnerLog jobRunnerLog = null;
        
        try {
            JobRunnerCLIParser commandLine = new JobRunnerCLIParser(args);
            
            JobRunner.pathToConfig = commandLine.getArgument("path_to_config");	
            if (JobRunner.pathToConfig == null) {
                JobRunner.pathToConfig = "./";
            } else if (!JobRunner.pathToConfig.endsWith("/")) {
                JobRunner.pathToConfig += "/";
            }
            
            // Get config.properties loaded in case we get an error and need to email alert
            prop.load(new FileInputStream(pathToConfig + "config.properties"));
            
            JobRunner.pathToScripts = commandLine.getArgument("path_to_scripts");
            if (JobRunner.pathToScripts == null) {
                JobRunner.pathToScripts = "../jobRunnerScripts/";
            } else if (!JobRunner.pathToScripts.endsWith("/")) {
                JobRunner.pathToScripts += "/";
            }
             
            ///*

            env = "production";
            region = "na";
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            start_day = df.parse("2015-02-11");
            end_day = df.parse("2015-02-12");
            jobs = "simpleBash.xml";
            dt_range = false;
            simulate = false;

            //*/
            /*
            env = commandLine.getArgument("env");
            if (env == null) {
                throw new Exception("The env argument is mandatory. \n" + JobRunnerCLIParser.getHelpMessage());
            }

            region = commandLine.getArgument("region");
            if (region == null) {
                throw new Exception("The region argument is mandatory.\n" + JobRunnerCLIParser.getHelpMessage());
            }

            
            simulate = (commandLine.getArgument("simulate") != null && 
                        commandLine.getArgument("simulate").toLowerCase().equals("true"));
            
            dt_range = (commandLine.getArgument("date_range") != null && 
                        commandLine.getArgument("date_range").toLowerCase().equals("true"));

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

            if (commandLine.getArgument("start_day") != null) {
                start_day = df.parse(commandLine.getArgument("start_day"));
            } 

            if (commandLine.getArgument("end_day") != null) {
                end_day = df.parse(commandLine.getArgument("end_day"));
            }
            jobs = commandLine.getArgument("jobs");
            if (jobs == null) {
                jobs = "jobs.xml";
            }
            */
            StringBuilder msg = new StringBuilder("Running : \n");
            msg.append("env: ").append(env).append("\n").
                    append("region: ").append(region).append("\n").
                    append("start_day: ").append(start_day).append("\n").
                    append("end_day: ").append(end_day).append("\n").
                    append("dt_range: ").append(dt_range).append("\n").
                    append("jobs: ").append(jobs).append("\n");

            LoggerHelper.logInfo(JobRunner.class.getName(), msg.toString());

            region = region.toLowerCase();
            if (region.equals("intl")) {
                region = "int";
            }
            
            st_day =  start_day != null ? df.format(start_day) : "";
            en_day = end_day != null ? df.format(end_day) : "";
            
            JobManager.createInstance().setParams(env, st_day, en_day, region, simulate);
            
            jobRunnerLog = new JobRunnerLog(jobs, null);
        
            jobRunnerLog.insertUpdate(JobRunnerLog.JobRunnerStatus.RUNNING);
            
            subject = region + ", " + env + ", " + st_day + " - " + en_day + ", range=" + dt_range.toString() + ", simulate=" + simulate.toString() + ", " + jobs;
            
            Utility.sendAlert(prop.getProperty(env + ".alertTo"), "", startingMsg + subject );
            
            updateJobRunner(env, start_day, end_day, dt_range, region, jobs, simulate);
            
            status = JobRunnerLog.JobRunnerStatus.SUCCESS;
            subject = successMsg + subject;
            body = "";
        }
        catch(Exception ex) {
           
            subject = criticalMsg + subject;
            String message = (ex.getCause() != null) ? ex.getCause().getMessage() : ex.getMessage();
            body = "\nstart_day = " + st_day + 
                          "\nend_day = " + en_day + 
                          "\nenv =  " + env +
                          "\nregion = " + region +
                          "\nException = " + message;
            body = body.concat("\nStack trace = ");
            for (StackTraceElement el : ex.getStackTrace()){
                body = body.concat("\n\t" + el.toString());
            }
            LoggerHelper.logError(JobRunner.class.getName(), 
                                    subject + body);
            
            if (jobRunnerLog != null) {
                jobRunnerLog.setErrorMessage(message);
                status = JobRunnerLog.JobRunnerStatus.FAILED;
            }
            throw ex;
        }
        finally {
            Utility.sendAlert(prop.getProperty(env + ".alertTo"), body, subject);
            if (jobRunnerLog != null) {
                jobRunnerLog.insertUpdate(status);
            }
        }

        LoggerHelper.logInfoEnd(JobRunner.class.getName()); 
    }
}
