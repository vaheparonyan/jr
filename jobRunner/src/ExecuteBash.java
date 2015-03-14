
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
public class ExecuteBash extends JobObject{

    public ExecuteBash(List<Object> par, String name, String onError, Asserter asserter){
        super(par, name, onError, asserter);
    }
    
    protected String command = null, outputFile = null;
    protected HashMap params = null;
    
    @Override
    public void setProperties(HashMap params, String command, String outputFile) {
        this.command = command;
        this.params = params;
        this.outputFile = outputFile;
    }
    
    @Override
    public synchronized void execute() throws Exception {
        LoggerHelper.logInfoStart(ExecuteBash.class.getName());
        if (!JobManager.createInstance().simulate) {
                    
            if ( this.command == null || this.outputFile == null || this.params == null) {
                throw new Exception ("The properties are not set - command: " + this.command + 
                                                                   ", outputFile : " + this.outputFile + 
                                                                   ", params : " + this.params.toString() );
            }

            command = Utility.substituteParameters(command, params);

            Runtime run = Runtime.getRuntime();
            LoggerHelper.logInfo(ExecuteBash.class.getName(), "Running shell command : \n" + command);

            Process proc = run.exec(new String[]{"/bin/bash", "-c", command});

            BufferedReader buf;
            String data;

            if (!outputFile.isEmpty()) {
                Writer writer = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outputFile), "utf-8"));

                Utility.copy(new InputStreamReader(proc.getInputStream()), writer);            

                writer.flush();
            } else {
                // just read buffer
                Utility.copy(new InputStreamReader(proc.getInputStream()), null);
            }

            proc.waitFor();

            if (proc.exitValue() != 0) {

                data = "The Execution of bash command failed : " + proc.exitValue() + " : ";

                buf = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                String line ;
                while ( (line = buf.readLine()) != null) {
                    data += line + "\n";
                }

                throw new Exception(data);
            }
        }
        LoggerHelper.logInfoEnd(ExecuteBash.class.getName());
    }
}
