import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author vparonyan
 */
public class JobManager {
    
    static private JobManager instance = null;
    private ExecutorService executor = null;
    protected String env = "";
    protected String date = "";	// in format of "yyyy-MM-dd"
    protected String endDate = "";	// in format of "yyyy-MM-dd"
    protected String region;
    protected Properties prop = new Properties();
    protected Boolean simulate = false;

    public static JobManager createInstance() throws Exception{
        //LoggerHelper.logInfoStart(JobManager.class.getName());
        if (instance == null) {
            instance = new JobManager();
        }
        //LoggerHelper.logInfoEnd(JobManager.class.getName());
        
        return instance;    
    }

    private JobManager() throws Exception{
        LoggerHelper.logInfoStart(JobManager.class.getName());

        this.prop.load(new FileInputStream(JobRunner.pathToConfig + "config.properties"));
        
        LoggerHelper.logInfoEnd(JobManager.class.getName());
    }
    
    public ExecutorService getExecutorService() {
        return executor;
    }
    
    public void setParams(String env, String date, String endDate, String region, Boolean simulate) {
        this.env = env;
        this.date = date;
        this.endDate = endDate;
        this.region = region;
        this.simulate = simulate;
    }
    
    private void executeParentJobs() throws Exception {
        LoggerHelper.logInfoStart(JobManager.class.getName());
        
        List<RunCommand> tasks = new ArrayList();
        executor = Executors.newFixedThreadPool(16);
        ExecutorCompletionService ecs = new ExecutorCompletionService(executor);
        
        for (Map.Entry<String, JobObject> p : 
                            JobRunnerXMLParser.createInstance().getJobs().entrySet()) {
            if (!p.getValue().isChild()){
                RunCommand command = new RunCommand(p.getValue());

                ecs.submit(command, p.getValue().getName());
                tasks.add(command);
            }
        } 

        for(int i = 0; i < tasks.size(); i++) {
            ecs.take().get();
        }
        
        executor.shutdown();
        executor=null;
        
        LoggerHelper.logInfoEnd(JobManager.class.getName());
    }
    
    private void handleJobErrors() throws Exception {
        LoggerHelper.logInfoStart(JobManager.class.getName());
        
        for (Map.Entry<String, JobObject> p : 
                            JobRunnerXMLParser.createInstance().getJobs().entrySet()) {
            if (!p.getValue().isChild() && p.getValue().getException() != null) {
                throw p.getValue().getException();
            }
        }
        LoggerHelper.logInfoEnd(JobManager.class.getName());
    }
    
    private void executeJobs() throws Exception{
        LoggerHelper.logInfoStart(JobManager.class.getName());
        
        executeParentJobs();
        
        handleJobErrors();

        LoggerHelper.logInfoEnd(JobManager.class.getName());
    }
    
    private void markChildJobs() throws Exception {
       for (Map.Entry<String, JobObject> p : 
                            JobRunnerXMLParser.createInstance().getJobs().entrySet()) {
            for (String n : p.getValue().getDependentObjects()) {
                JobObject j = null;
                if (JobRunnerXMLParser.createInstance().getJobs().containsKey(n)) {
                    j = JobRunnerXMLParser.createInstance().getJobs().get(n);

                } else {
                    throw new Exception("The job with name '" + n + "' does not exist(" + p.getValue().getName() + ").");
                }
                j.isChild(true);
            }
        } 
    }
    
    public void execute(String xmlFileName) throws Exception {
        LoggerHelper.logInfoStart(JobManager.class.getName());

        LoggerHelper.logInfo(JobManager.class.getName(), xmlFileName);
        JobRunnerXMLParser.createInstance().createJobStack(xmlFileName);
        
        markChildJobs();
        executeJobs();

        LoggerHelper.logInfoEnd(JobManager.class.getName());
    }
    
}
