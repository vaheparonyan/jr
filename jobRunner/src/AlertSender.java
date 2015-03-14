import java.util.HashMap;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vparonyan
 */
public class AlertSender extends JobObject{

    private String to = null, body = null, subject = null, attachment = null;
    public static enum AlertType{OnStart, OnSuccess, OnError};
    private final AlertType type;
    
    public AlertSender(List<Object> par, String name, String onError, Asserter asserter, AlertType type) {
        super(par, name, onError, asserter);
        this.type = type;
    }

    public void setProperties(String to, String body, String subject, String attachment) {
        this.to = to;
        this.body = body;
        this.subject = subject;
        this.attachment = attachment;
        
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(AlertSender.class.getName());
        if (!JobManager.createInstance().simulate) {
                     
            if ( this.to == null || this.body == null || this.subject == null || this.attachment == null) {
                throw new Exception ("The properties are not set - to: " + this.to + 
                                                                   ", body : " + this.body + 
                                                                   ", subject : " + this.subject + 
                                                                   ", attachment : " + this.attachment);
            }

            ExecuteBash bash = new ExecuteBash(null, null, JobRunnerXMLParser.ReservedKeywords.FAIL.toString().toLowerCase(), null);
            String command = "echo \"" + this.body + "\" | mailx -s \"" + subject;
            if (this.attachment != null && !this.attachment.isEmpty()) {
                command += " -a \"" + this.attachment + "\"";
            }
            command += this.to;
            bash.setProperties(new HashMap(), command, "");
            bash.execute();
        }
        LoggerHelper.logInfoEnd(AlertSender.class.getName());
    }
}
