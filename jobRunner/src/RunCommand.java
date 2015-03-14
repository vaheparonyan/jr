import java.lang.reflect.Method;

/**
 *
 * @author vparonyan
 */
public class RunCommand implements Runnable {
    public final JobObject jobObject;

    public RunCommand(JobObject obj) {
        this.jobObject = obj;
    }

    private void waitForChildren() throws Exception {
        
        for (String n : jobObject.getDependentObjects()) {
            if (JobRunnerXMLParser.createInstance().getJobs().containsKey(n)) {
                JobObject p = JobRunnerXMLParser.createInstance().getJobs().get(n);
                if (p.getFuture() != null) {
                    // the following get method hangs the current thread until the dependent thread is running
                    p.getFuture().get();
                }
            }
        }
        
        for (String n : jobObject.getDependentObjects()) {
            if (JobRunnerXMLParser.createInstance().getJobs().containsKey(n)) {
                JobObject p = JobRunnerXMLParser.createInstance().getJobs().get(n);
                if (p.getException() != null) {
                    throw p.getException();
                }
            }
        }
    }
    
    private synchronized void createAndSetFuture(JobObject p) throws Exception {
        if (p.getFuture() == null) {
            RunCommand command = new RunCommand(p);

            p.setFuture(JobManager.createInstance().getExecutorService().submit(command));
        }
    }
    
    private synchronized void executeChildren() throws Exception {
        
        for (String n : jobObject.getDependentObjects()) {
            JobObject p = null;
            if (JobRunnerXMLParser.createInstance().getJobs().containsKey(n)) {
                p = JobRunnerXMLParser.createInstance().getJobs().get(n);

            } else {
                throw new Exception("The job with name '" + n + "' does not exist(" + jobObject.getName() + ").");
            }
            
            createAndSetFuture(p);
        }
    }
    
    private void execute() throws Exception {
        
        int size = jobObject.getParameters() == null ? 0 : jobObject.getParameters().size();
        Class[] param_types = new Class[size];
        Object[] params = new Object[size];

        StringBuilder msg;

        Method m, propM;

        msg = new StringBuilder();
        msg.append(jobObject.getClass().getName()).append("::").append(jobObject.getMethod());
        for (int i = 0; i < size; ++i) {
            param_types[i] = jobObject.getParameters().get(i).getClass();
            params[i] = jobObject.getParameters().get(i);
            msg.append(", ");
            msg.append(params[i].toString());
        }

        LoggerHelper.logInfo(RunCommand.class.getName(), msg.toString());

        m = jobObject.getClass().getDeclaredMethod(jobObject.getMethod());
        
        if (size == 0) {
            propM = jobObject.getClass().getDeclaredMethod(jobObject.getPropertiesMethod());
        } else {
            propM = jobObject.getClass().getDeclaredMethod(jobObject.getPropertiesMethod(), param_types);
        }
        
        int retryNum = 1;
        int retryPeriod = 0;
        if ("retry".equals(jobObject.getOnErrorAction())) {
            retryNum = jobObject.getRetryNum();
            retryPeriod = jobObject.getRetryPeriod();
        }
        
        for (int i = 0; i < retryNum; i++)  {
            try {
                propM.invoke(jobObject, params);
                m.invoke(jobObject, new Object[0]);
                // if no exception then break loop
                break;
            } catch (Exception ex){
                LoggerHelper.logInfo(RunCommand.class.getName(), "Attempt #" + i + ". Exception is " + ex.getMessage());
                if (i + 1 < retryNum) {
                    Thread.sleep(1000*i*retryPeriod);
                } else {
                    throw ex;
                }
            }
        }
    }
    
    @Override
    public void run() {
        JobRunnerLog jobrunnerLog = new JobRunnerLog(this.jobObject.getName(), "");

        try {
            jobrunnerLog.insertUpdate(JobRunnerLog.JobRunnerStatus.AWAITING);

            executeChildren();

            waitForChildren();
            
            jobrunnerLog.insertUpdate(JobRunnerLog.JobRunnerStatus.RUNNING);
            
            execute();
            
            jobrunnerLog.insertUpdate(JobRunnerLog.JobRunnerStatus.SUCCESS);
            
        } catch (Exception ex) {
            handleException(ex, jobrunnerLog);
        }
    }

    private void handleException(Exception ex, JobRunnerLog jobrunner_log) {
        // this function should be called only if exception happens 
        assert(ex != null);

        String message = (ex.getCause() != null) ? ex.getCause().getMessage() : ex.getMessage(); 
        switch (jobObject.getOnErrorAction()) {
            case "fail":
                jobObject.setException(ex);
                break;
            case "ignore":
                break;
            default:
                jobObject.setException(ex);
                break;
        }
        
        try {    
            if (jobrunner_log.getStatus() == JobRunnerLog.JobRunnerStatus.AWAITING) {
                jobrunner_log.insertUpdate(JobRunnerLog.JobRunnerStatus.PENDED);
            } else {                
                LoggerHelper.logError(RunCommand.class.getName(), jobObject.getName() + ": " + message); 
                jobrunner_log.setErrorMessage(message);
                jobrunner_log.insertUpdate(JobRunnerLog.JobRunnerStatus.FAILED);
            }
        } catch(Exception exLog) {
            try {
                LoggerHelper.logError(RunCommand.class.getName(), (exLog.getCause() != null) ? exLog.getCause().getMessage() : exLog.getMessage()); 
            } catch (Exception e) {
                jobObject.clearException();
                jobObject.setException(exLog);
            }                   
        }
    }
}