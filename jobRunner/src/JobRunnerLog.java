
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author vparonyan
 */
public class JobRunnerLog {
    public static enum JobRunnerStatus {FAILED, AWAITING, RUNNING, SUCCESS, PENDED, UNKNOWN};
    
    protected String start_date = null;
    protected String end_date = null;
    protected String file_name = null;
    protected String machine_name = null;
    protected String user_name = null;
    protected String start_time = null;
    protected String end_time = null;
    protected JobRunnerStatus status = null;
    protected String error_msg = null;
    protected String job_name = null;
    protected String job_content = null;
    
    static protected String parent_id = null;
    
    public JobRunnerLog(String file_name, String job_content) {

        this.file_name = file_name;

        try
        {
            setMachineName(InetAddress.getLocalHost().getHostName());
        }
        catch (UnknownHostException ex)
        {
            setMachineName("Unknown");
        }
        setUserName(System.getProperty("user.name"));
        
        this.start_time = "";
        this.end_time = "";
        this.status = JobRunnerStatus.UNKNOWN;
        this.error_msg = null;
        this.setJobName(Thread.currentThread().getName());
        this.setJobContent(job_content);
    }

    public String getStartDate() {
        return this.start_date;
    }
     
    public String getEndDate() {
        return this.end_date;
    }
         
    public JobRunnerStatus getStatus() {
        return this.status;
    }

    public void setStatus(JobRunnerStatus st) throws Exception{
        if ((this.status == JobRunnerStatus.FAILED || this.status == JobRunnerStatus.SUCCESS || this.status == JobRunnerStatus.PENDED)  ||
                
            (this.status == JobRunnerStatus.UNKNOWN && 
                (st != JobRunnerStatus.AWAITING && st != JobRunnerStatus.RUNNING)) ||
                
             (this.status == JobRunnerStatus.AWAITING && 
                (st != JobRunnerStatus.FAILED && st != JobRunnerStatus.RUNNING && st != JobRunnerStatus.PENDED)) ||
                
             (this.status == JobRunnerStatus.RUNNING && 
                (st != JobRunnerStatus.FAILED && st != JobRunnerStatus.SUCCESS))) {
            
            throw new Exception("Incorrect status is set: " + this.status.toString() + " to " + st.toString());
        }
        if (this.status == JobRunnerStatus.UNKNOWN) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            this.start_time = df.format(new Date(Calendar.getInstance().getTimeInMillis()));
        } else if (this.status == JobRunnerStatus.RUNNING) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            this.end_time = df.format(new Date(Calendar.getInstance().getTimeInMillis()));
        }
        this.status = st;
    }
    
    public String getFileName() {
        return this.file_name;
    }
    
    public void setFileName(String file_name) {
        this.file_name = file_name;
    }
    
    public String getMachineName() {
        return this.machine_name;
    }
    
    private void setMachineName(String machine_name) {
        this.machine_name = machine_name;
    }
    
    public String getUserName() {
        return this.user_name;
    }
    
    private void setUserName(String user_name) {
        this.user_name = user_name;
    }
    
    public String getErrorMessage() {
        return this.error_msg;
    }
    
    public void setErrorMessage(String error_msg) {
        this.error_msg = error_msg;
    }
     
    public String getJobName() {
        return this.job_name;
    }
    
    private void setJobName(String job_name) {
        this.job_name = job_name;
    }
    
    public String getJobContent() {
        return this.job_content;
    }
    
    private void setJobContent(String job_content) {
        this.job_content = job_content;
    }
    
    protected void setParentId() {
        if (this.job_name.equals("main")) {
            parent_id = "[" + this.user_name + "][" + this.machine_name + "][" + this.start_time + "][" + this.job_name + "]";
        }
    }
    
    public void insertUpdate(JobRunnerLog.JobRunnerStatus status) throws Exception {
        setStatus(status);
        setParentId();
        
        HashMap queryParameters = new HashMap();
        queryParameters.put("env", JobManager.createInstance().env);
        queryParameters.put("region", JobManager.createInstance().region);
        queryParameters.put("start_date", JobManager.createInstance().date);
        queryParameters.put("end_date", JobManager.createInstance().endDate);
        queryParameters.put("file_name", this.file_name);
        queryParameters.put("machine_name", this.machine_name);
        queryParameters.put("user_name", this.user_name);
        queryParameters.put("start_time", this.start_time);
        queryParameters.put("end_time", this.end_time);
        queryParameters.put("status", this.status.toString().toLowerCase());
        queryParameters.put("error_msg", this.error_msg == null ? "" : this.error_msg.replace('\"', '\''));
        queryParameters.put("job_name", this.job_name);
        queryParameters.put("job_content", this.job_content == null ? "" : this.job_content.replace('\"', '\''));
        queryParameters.put("parent", JobRunnerLog.parent_id);
        queryParameters.put("id", "[" + this.user_name + "][" + this.machine_name + "][" + this.start_time + "][" + this.job_name + "]");
        

        JobObject j = new SQLJobObject(null, null, JobRunnerXMLParser.ReservedKeywords.FAIL.toString().toLowerCase(), null);
        j.setProperties(queryParameters, "jobrunner_log", JobRunnerConnection.ConnectionType.MYSQL);
        j.execute();
    }
}
