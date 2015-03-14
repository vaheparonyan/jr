
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
public class HiveJobObject extends ExecuteBash{

    public HiveJobObject(List<Object> par, String name, String onError, JobObject.Asserter asserter){
        super(par, name, onError, asserter);
    }
    
    @Override
    public void setProperties(HashMap params, String command, HashMap hiveArgs) {
        String hiveCommand = "hive -S";
        for (Object key: hiveArgs.keySet()) {              
            switch (JobRunnerXMLParser.ReservedKeywords.valueOf(key.toString())) {
                case DATABASE:
                    if (hiveArgs.get(key) != null) {
                        hiveCommand += " --database " + hiveArgs.get(key).toString();
                    }
                    break;
                case HOSTNAME:
                    if (hiveArgs.get(key) != null) {
                        hiveCommand += " -h " + hiveArgs.get(key).toString();
                    }
                    break;
                case PORT:
                    if (hiveArgs.get(key) != null) {
                        hiveCommand += " -p " + hiveArgs.get(key).toString();
                    }
                    break;
                case QUERY_TYPE:
                    
                    if (hiveArgs.get(key).toString().equals(
                            JobRunnerXMLParser.ReservedKeywords.COMMAND.toString().toLowerCase())) {
                        hiveCommand += " -e \"" + command + "\"";
                    } else { //JobRunnerXMLParser.ReservedKeywords.FILE.toString().toLowerCase())
                        hiveCommand += " -f " + command;
                    }
                    break;
                case INITIALIZATION:
                    if (hiveArgs.get(key) != null) {
                        hiveCommand += " -i " + hiveArgs.get(key).toString();
                    }
                    break;
                case HIVECONF:
                    String properties = "";
                    HashMap propertyMap = (HashMap)hiveArgs.get(key);
                    for (Object propertyKey: propertyMap.keySet()) {
                        properties += " --hiveconf " + propertyKey.toString() + 
                                        "=" + propertyMap.get(propertyKey);
                    }
                    hiveCommand += properties;
                    break;
                case HIVEVAR:
                    String havevars = "";
                    HashMap havevarMap = (HashMap)hiveArgs.get(key);
                    for (Object havevarKey: havevarMap.keySet()) {
                        havevars += " --hivevar " + havevarKey.toString() + 
                                        "=" + havevarMap.get(havevarKey);
                    }
                    hiveCommand += havevars;
                    break;
                default:
                    assert false;//  ("Invalid parameter in " + name + " job for hive command " + key.toString());
            }
        }
        setProperties(params, hiveCommand, "");
    }
    
    @Override
    public synchronized void execute() throws Exception {
        LoggerHelper.logInfoStart(ExecuteBash.class.getName());
        super.execute();
        LoggerHelper.logInfoEnd(ExecuteBash.class.getName());
    }
}
