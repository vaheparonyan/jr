import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 * @author vparonyan
 */
public class JobRunnerXMLParser {

    private static final boolean noLogInfo = true;
    
    protected static enum ReservedKeywords{JOB, DEPENDENCIES, DEPENDENT, NAME, REGION, FAIL, TYPE, 
            ONERROR, RETRY, RETRYPERIOD, FILE, COMMAND, 
            DATABASE, HOSTNAME, INITIALIZATION, HIVECONF, HIVEVAR, PORT, QUERY_TYPE, VARIABLES, PROPERTY,
            INTERNALFUNCTION, EQUAL_NOTEQUAL, PARAMETERS, KEYVALUE, KEY, VALUE, DATE, DATEKEY, DBNAME, 
            DATASHORTKEY, MYSQL, COLUMN, COLUMNS, START_DT, END_DT, RESULTSET, OUTPUTFILE, CONNECTIONTYPE, 
            ASSERTER, DATATYPE, CONDITION, LESSER, EQUAL, GREATER, VARCHAR, INTEGER, IGNORE, DTKEY, OVERRIDE };

    protected static enum ReservedFunctionName{
        createTable, dummyJob, executeSimpleQuery, executeQuery, executeHiveQuery, dropTable, executeBash, brightEdge, 
        getTaxonomies, getSiteMapUrls, keywordStats, sendAlert
    };
    
    private static final String CONFIG_PROPERTIES = JobRunner.pathToConfig + "config.properties";
    static private JobRunnerXMLParser instance = null;
    
    protected Properties prop = new Properties();
    
    public static JobRunnerXMLParser createInstance() throws Exception{
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);
        if (instance == null) {
            instance = new JobRunnerXMLParser();
        }
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
        return instance;    
    }
    
    private JobRunnerXMLParser() throws IOException, ClassNotFoundException {
        this.prop.load(new FileInputStream(CONFIG_PROPERTIES));
        
        this.functionToClass = new HashMap();
        this.functionToClass.put(ReservedFunctionName.executeSimpleQuery.toString(), SQLJobObject.class);
        this.functionToClass.put(ReservedFunctionName.executeQuery.toString(), SQLJobObject.class);
        this.functionToClass.put(ReservedFunctionName.executeHiveQuery.toString(), HiveJobObject.class);
        this.functionToClass.put(ReservedFunctionName.executeBash.toString(), ExecuteBash.class);
        this.functionToClass.put(ReservedFunctionName.dropTable.toString(), SQLDropTableJobObject.class);
        this.functionToClass.put(ReservedFunctionName.createTable.toString(), SQLCreateTableJobObject.class);
        this.functionToClass.put(ReservedFunctionName.brightEdge.toString(), BrightEdgeDataCollector.class);
        this.functionToClass.put(ReservedFunctionName.keywordStats.toString(), GoogleAdKeywordStatsJobObject.class);
        this.functionToClass.put(ReservedFunctionName.getTaxonomies.toString(), TaxonomyReader.class);
        this.functionToClass.put(ReservedFunctionName.getSiteMapUrls.toString(), SiteMapUrls.class);
        this.functionToClass.put(ReservedFunctionName.sendAlert.toString(), AlertSender.class);
    }
    
    private final HashMap<String, JobObject> jobobjects = new HashMap();
    private final HashMap<String, List<String>> resultSetParameters = new HashMap();
  
    private final HashMap<String, Class> functionToClass;
    
    public void createJobStack(String xmlFileName) throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);
        
        clearJobObjects();

        // parse the xml and create the job stack
        File file = new File(xmlFileName);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(file);
        
        Element rootEl = document.getDocumentElement();
        
        initKeyFieldNames(rootEl);
        
        createJobElements(rootEl);

        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
    }
    
    private HashMap<String, String> getAttributes(Element el) {
        HashMap ret = new HashMap();
        
        NamedNodeMap l = el.getAttributes();
        for (int i = 0; i < l.getLength(); i++) {
            if (l.item(i).getNodeType() == Node.ATTRIBUTE_NODE) {
                
                ret.put(l.item(i).getNodeName().toLowerCase(), l.item(i).getNodeValue());
            }
        }
        return ret;
    }
            
    private void createJobElements(Element rootEl)  throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);

        // get all jobs in current root
        List<Element> nList = getChildList(rootEl, ReservedKeywords.JOB.toString().toLowerCase());
        for (Element el: nList) {
            HashMap<String, String> attributes = this.getAttributes(el);
            
            String reg = attributes.get(ReservedKeywords.REGION.toString().toLowerCase());
            if(reg.isEmpty()) {
                throw new Exception("Invalid tag in xml, region attribute does not exist for job tag");
            }

            List<String> regions = Arrays.asList((reg).split(","));
            if (regions.indexOf(JobManager.createInstance().region) == -1) {
                // if the xml job is not created for passed region then ignore this job 
                continue;
            }

            String onError = getJobActionOnError(attributes);
            
            List<Object> params = new ArrayList();

            getJobParameters(el, params);
            
            String functionName = attributes.get(ReservedKeywords.TYPE.toString().toLowerCase());
            
            String name = attributes.get(ReservedKeywords.NAME.toString().toLowerCase());

            String command = getCommandOrFile(attributes);
            
            // as we already get the parameters, lets substitude them in name, functionName, command and onError
            name = Utility.substituteParameters(name, (HashMap)params.get(0));
            functionName = Utility.substituteParameters(functionName, (HashMap)params.get(0));
            onError = Utility.substituteParameters(onError, (HashMap)params.get(0));
            
            if (command != null) {
                command = Utility.substituteParameters(command, (HashMap)params.get(0));
                params.add(command);
            }
            
            if(functionName.isEmpty()) {
                throw new Exception("Invalid tag in xml, name attribute does not exist for job tag");
            }
            
            if(name.isEmpty()) {
                throw new Exception("Invalid tag in xml, name attribute does not exist for job tag");
            }

            if (functionName.equals(ReservedFunctionName.executeSimpleQuery.toString())) {
                String dbtype = attributes.get(ReservedKeywords.CONNECTIONTYPE.toString().toLowerCase());
                // as we already get the parameters, lets substitude them in dbtype
                dbtype = Utility.substituteParameters(dbtype, (HashMap)params.get(0));
                if(dbtype.isEmpty()) {
                    throw new Exception("Invalid tag in xml, connectionType attribute does not exist for job tag");
                }
                params.add(JobRunnerConnection.getConnectionType(dbtype));

            } else if (functionName.equals(ReservedFunctionName.executeQuery.toString())) {
                String dbtype = attributes.get(ReservedKeywords.CONNECTIONTYPE.toString().toLowerCase());
                // as we already get the parameters, lets substitude them in dbtype
                dbtype = Utility.substituteParameters(dbtype, (HashMap)params.get(0));
                if(dbtype.isEmpty()) {
                    throw new Exception("Invalid tag in xml, connectionType attribute does not exist for job tag");
                }

                params.add(JobRunnerConnection.getConnectionType(dbtype));
                
                // addResultSetFields will return the 'exceptional' column names, so these names will overide the names in resultset
                HashMap<String, String> overrides = new HashMap();
                String resultSetName = addResultSetFields(el, params, overrides);
                
                if (!resultSetParameters.containsKey(resultSetName)) {
                    throw new Exception("Could not find the result set named: " + resultSetName);
                }

                for (int i = 0; i < resultSetParameters.get(resultSetName).size(); i++) {
                    String paramName = resultSetParameters.get(resultSetName).get(i);
                    String overrideName = paramName;
                    if (overrides.containsKey(paramName)){
                        overrideName = overrides.get(paramName);
                    }
                    resultSetParameters.get(resultSetName).set(i, overrideName);
                }
                params.add(resultSetParameters.get(resultSetName));
                
            } else if (functionName.equals(ReservedFunctionName.dropTable.toString())) {
                String dbtype = attributes.get(ReservedKeywords.CONNECTIONTYPE.toString().toLowerCase());
                // as we already get the parameters, lets substitude them in dbtype
                dbtype = Utility.substituteParameters(dbtype, (HashMap)params.get(0));
                if(dbtype.isEmpty()) {
                    throw new Exception("Invalid tag in xml, connectionType attribute does not exist for job tag");
                }
                params.add(JobRunnerConnection.getConnectionType(dbtype));
            } else if (functionName.equals(ReservedFunctionName.createTable.toString())) {
                LinkedHashMap columns = getColumns(el);
                // as we already get the parameters, lets substitude them in columns
                LinkedHashMap newColumns = new LinkedHashMap();
                for (Object entry : columns.entrySet()) {
                    String key = ((Map.Entry<String, String>)entry).getKey();
                    key = Utility.substituteParameters(key, (HashMap)params.get(0));
                    String type = ((Map.Entry<String, String>)entry).getValue();
                    type = Utility.substituteParameters(type, (HashMap)params.get(0));
                    
                    newColumns.put(key, type);
                }
                
                params.add(newColumns);
                
                String dbtype = attributes.get(ReservedKeywords.CONNECTIONTYPE.toString().toLowerCase());
                // as we already get the parameters, lets substitude them in dbtype
                dbtype = Utility.substituteParameters(dbtype, (HashMap)params.get(0));
                if(dbtype.isEmpty()) {
                    throw new Exception("Invalid tag in xml, connectionType attribute does not exist for job tag");
                }
                params.add(JobRunnerConnection.getConnectionType(dbtype));
            } else if (functionName.equals(ReservedFunctionName.executeBash.toString())) {
                String outFile = attributes.get(ReservedKeywords.OUTPUTFILE.toString().toLowerCase());
        
                if (outFile == null) {
                    outFile = "";
                }
                // as we already get the parameters, lets substitude them in outFile
                outFile = Utility.substituteParameters(outFile, (HashMap)params.get(0));
                params.add(outFile);
            } else if (functionName.equals(ReservedFunctionName.executeHiveQuery.toString())) {
                /*
                -d,--define <key=value>          Variable subsitution to apply to hive
                                  commands. e.g. -d A=B or --define A=B
                //--database <databasename>     Specify the database to use
                //-e <quoted-query-string>         SQL from command line
                //-f <filename>                    SQL from files
                //-H,--help                        Print help information
                //-h <hostname>                    connecting to Hive Server on remote host
                   --hiveconf <property=value>   Use value for given property
                   --hivevar <key=value>         Variable subsitution to apply to hive
                                                 commands. e.g. --hivevar A=B
                //-i <filename>                    Initialization SQL file
                //-p <port>                        connecting to Hive Server on port number
                -S,--silent                      Silent mode in interactive shell
                //-v,--verbose                     Verbose mode (echo executed SQL to the
                                             console)
                
                <job name="hive job name" command|file="kb_ks_keyword_ideas" 
                    region="int|na|needish" onerror="Ignore|FAIL|RETRY"
                    database="databasename(by default not specified)" hostname="hostname(by default not specified)" port="port(by default not specified)" 
                    query_type="file|command 
                    initialisation="Initialization SQL file">
                    
                    <variables>
                        <hiveconf property="date" value="${date}"/>
                        <hivevar key="dbname" value="${dbname}"/>
                        <hiveconf property="equal_notequal" value="${equal_notequal}"/>
                        <hivevar key="tablename" value="int_performance_reporting"/>
                   </variables>
                   <parameters>
                       <keyvalue key="paramname" value="paramvalue"/>
                   </parameters>
                   <dependencies>
                        <dependent name='dependency name'/>
                   </dependencies>
               </job>
                */
                HashMap hiveArgs = new HashMap();               
                hiveArgs.put(ReservedKeywords.DATABASE.toString(), attributes.get(ReservedKeywords.DATABASE.toString().toLowerCase()));
                hiveArgs.put(ReservedKeywords.HOSTNAME.toString(), attributes.get(ReservedKeywords.HOSTNAME.toString().toLowerCase()));
                hiveArgs.put(ReservedKeywords.PORT.toString(), attributes.get(ReservedKeywords.PORT.toString().toLowerCase()));
                                
                String querytype = attributes.get(ReservedKeywords.QUERY_TYPE.toString().toLowerCase());
                
                if (querytype == null || 
                        (!querytype.equals(ReservedKeywords.FILE.toString().toLowerCase()) && 
                         !querytype.equals(ReservedKeywords.COMMAND.toString().toLowerCase()))) {
                    throw new Exception("Invalid attribute in " + name + " job, the " + 
                            ReservedKeywords.QUERY_TYPE.toString().toLowerCase() + " is " + querytype + ".");
                }
                hiveArgs.put(ReservedKeywords.QUERY_TYPE.toString(), querytype);

                hiveArgs.put(ReservedKeywords.INITIALIZATION.toString(), 
                        attributes.get(ReservedKeywords.INITIALIZATION.toString().toLowerCase()));

                List<Element> vList = getChildList(el, ReservedKeywords.VARIABLES.toString().toLowerCase());
                if (vList.size() > 1) {
                    throw new Exception("Invalid number of variable tags in " + name + " job.");
                }
                
                HashMap hostvarMap = new HashMap();
                HashMap propertyMap = new HashMap();
                for (Element varEl: vList) {
                    /*
                    <hiveconf property="date" value="${date}"/>
                    <hivevar key="dbname" value="${dbname}"/>
                    <hiveconf property="equal_notequal" value="${equal_notequal}"/>
                    <hivevar key="tablename" value="int_performance_reporting"/>
                    */
                    List<Element> pList = getChildList(varEl, ReservedKeywords.HIVECONF.toString().toLowerCase());
                    for (Element pEl: pList) {

                        HashMap<String, String> attrbs = this.getAttributes(pEl);
                        String property = attrbs.get(ReservedKeywords.PROPERTY.toString().toLowerCase());
                        String value = attrbs.get(ReservedKeywords.VALUE.toString().toLowerCase());
                        if (property == null || value == null) {
                            throw new Exception("Invalid " + ReservedKeywords.HIVECONF.toString().toLowerCase() + " property in " + name + " job.");
                        }
                        propertyMap.put(property, value);
                    }
                    List<Element> hvList = getChildList(varEl, ReservedKeywords.HIVEVAR.toString().toLowerCase());
                    for (Element hvEl: hvList) {
                        HashMap<String, String> attrbs = this.getAttributes(hvEl);
                        String key = attrbs.get(ReservedKeywords.KEY.toString().toLowerCase());
                        String value = attrbs.get(ReservedKeywords.VALUE.toString().toLowerCase());
                        if (key == null || value == null) {
                            throw new Exception("Invalid " + ReservedKeywords.HIVEVAR.toString().toLowerCase() + " variable in " + name + " job.");
                        }
                        hostvarMap.put(key, value);
                    }
                }
                hiveArgs.put(ReservedKeywords.HIVECONF.toString(), propertyMap);
                hiveArgs.put(ReservedKeywords.HIVEVAR.toString(), hostvarMap);      
                
                params.add(hiveArgs);
                
            } else if (functionName.equals(ReservedFunctionName.brightEdge.toString())) {
                // everything we want is in parameters : start_dt, end_dt and outFile name
            } else if (functionName.equals(ReservedFunctionName.keywordStats.toString())) {
                // everything we want is in parameters : inFile name and outFile name
            } else if (functionName.equals(ReservedFunctionName.getTaxonomies.toString())) {
                // everything we want are in parameters taxonomy_guid
            } else if (functionName.equals(ReservedFunctionName.getSiteMapUrls.toString())) {
                // everything we want are in parameters date and outfile
            }
            
            JobObject.Asserter asserter = getJobAsserter(el);
            
            LoggerHelper.logInfo(JobRunnerXMLParser.class.getName(), this.getClass().getName() + " " + functionName);

            // todo, we can get in trouble if we add other ctors
            Constructor[] ctors = this.functionToClass.get(functionName).getDeclaredConstructors();

            if (ctors.length != 1) {
                throw new Exception("Invalid number of ctors, we should have only one ctor for " + 
                                        this.functionToClass.get(functionName).toString() );
            }
            JobObject p = (JobObject) ctors[0].newInstance(new Object[]{params, 
                                                                    name, 
                                                                    onError, 
                                                                    asserter});

            if (!jobobjects.containsKey(name.toLowerCase())){
                jobobjects.put(name.toLowerCase(), p);
            }
            createJobDependences(el, params, p);

        }
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
    }
    
    private void createJobDependences(Element el,  List<Object> par, JobObject parent) throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);

        for (Element e : getChildList(el,  ReservedKeywords.DEPENDENCIES.toString().toLowerCase())) {
            for (Element d : getChildList(e, ReservedKeywords.DEPENDENT.toString().toLowerCase())) {
                String name =  d.getAttribute(ReservedKeywords.NAME.toString().toLowerCase());        
                // as we already get the parameters, lets substitude them in outFile
                name = Utility.substituteParameters(name, (HashMap)par.get(0));
                
                if(name.isEmpty()) {
                    throw new Exception("Invalid tag in " + el.getAttribute(ReservedKeywords.NAME.toString().toLowerCase()) + 
                            "dependency, missing or empty name attribute");
                }
                
                parent.addDependency(name.toLowerCase());
            }
        }
   
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
    }
    
    // this function returns argument list for function that will be called 
    // in par and the index of parameters hashmap in that list. we need the 
    // parametrs to du substitution of name of job and etc.
    private void getJobParameters(Element jobEl, List<Object> par)  throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);

        addParameters(jobEl, par);
        
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
    }
    
    //        HashMap<String, String> attributes = this.getAttributes(jobEl);
    private String getJobActionOnError(HashMap<String, String> attributes)  throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);
        
        String onError = attributes.get(ReservedKeywords.ONERROR.toString().toLowerCase());
        if (onError != null && 
            !onError.toLowerCase().equals(ReservedKeywords.RETRY.toString().toLowerCase()) &&
            !onError.toLowerCase().equals(ReservedKeywords.IGNORE.toString().toLowerCase()) &&
            !onError.toLowerCase().equals(ReservedKeywords.FAIL.toString().toLowerCase())) {
            throw new Exception("Invalid tag in xml, incorrect onError attribute.");
        }
        
        if (onError == null || onError.toLowerCase().equals(ReservedKeywords.FAIL.toString().toLowerCase())) {
            onError = ReservedKeywords.FAIL.toString().toLowerCase();
        } else if (onError.toLowerCase().equals(ReservedKeywords.RETRY.toString().toLowerCase())) {
            String retrynum = attributes.get(ReservedKeywords.RETRY.toString().toLowerCase());
            String retryperiod = attributes.get(ReservedKeywords.RETRYPERIOD.toString().toLowerCase());
            if (retrynum.isEmpty() || retryperiod.isEmpty()) {
                throw new Exception("Invalid tag in xml, incorrect retry attribute.");
            } 
            onError += "," + retrynum;
            onError += "," + retryperiod;
        } else {
            onError = ReservedKeywords.IGNORE.toString().toLowerCase();
        }
        
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
        return onError;
    }
    
    private List<Element> getChildList(Element rootEl, String name) throws Exception {
        LoggerHelper.logInfoStart(JobRunnerXMLParser.class.getName(), noLogInfo);
        List<Element> nList = new ArrayList<>();
        
        for (Node child = rootEl.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (name.isEmpty() || name.toLowerCase().equals(child.getNodeName().toLowerCase())) {
                    nList.add((Element) child);
                }
            }
        }
        LoggerHelper.logInfoEnd(JobRunnerXMLParser.class.getName(), noLogInfo);
        return nList;
    }

    public HashMap<String, JobObject> getJobs() {
        return jobobjects;
    }
    
    private void clearJobObjects() throws Exception{
        jobobjects.clear();
    }
  
    private String getCommandOrFile(HashMap<String, String> attributes) throws Exception {
        /*Command
        <job name="executeSimpleQuery" command="clean_reporting" region="int,needish" connectionType="mysql">
        or
        <job name="executeSimpleQuery" file="clean_reporting" region="int,needish" connectionType="mysql">  
        */
        
        String file = attributes.get(ReservedKeywords.FILE.toString().toLowerCase());
        String command = attributes.get(ReservedKeywords.COMMAND.toString().toLowerCase());
        boolean hasCommand = command != null && !command.isEmpty();
        boolean hasFile = file != null && !file.isEmpty();
        
        if (hasCommand && hasFile) {
            throw new Exception("Missing command or file tags in " + attributes.get(ReservedKeywords.NAME.toString().toLowerCase()));
        } else if (!hasCommand && !hasFile) {
            throw new Exception("Expecting one command or file tags in " + attributes.get(ReservedKeywords.NAME.toString().toLowerCase()));
        } 

        command = !hasCommand ? file : command;

        if (command != null && !command.toLowerCase().equals(ReservedKeywords.INTERNALFUNCTION.toString().toLowerCase())) {
            return command;
        }
        return null;
    }
    
    // returns true if the job need to be ignored(the case when date is not 
    // specified by cli and parameter contains date variable), false - otherwise
    private void addParameters(Element jobEl,  List<Object> par) throws Exception {
        List<Element> pList = getChildList(jobEl, ReservedKeywords.PARAMETERS.toString().toLowerCase());
        if (pList.isEmpty()) {
            par.add(new HashMap());
        }
        
        for (Element parEl: pList) {
            /*
            <keyvalue key="date" value="${date}"/>
            <keyvalue key="dbname" value="${dbname}"/>
            <keyvalue key="equal_notequal" value="${equal_notequal}"/>
            <keyvalue key="tablename" value="int_performance_reporting"/>
            */
            List<Element> hList = getChildList(parEl, ReservedKeywords.KEYVALUE.toString().toLowerCase());
            HashMap parameter = new HashMap();

            String dt = "${" + ReservedKeywords.DATE.toString().toLowerCase() + "}";
            String dtkey = "${" + ReservedKeywords.DATEKEY.toString().toLowerCase() + "}";
            String dbname = "${" + ReservedKeywords.DBNAME.toString().toLowerCase() + "}";
            String mysql = ReservedKeywords.MYSQL.toString().toLowerCase();
            String region = "${" + ReservedKeywords.REGION.toString().toLowerCase() + "}";
            String eq_noteq = "${" + ReservedKeywords.EQUAL_NOTEQUAL.toString().toLowerCase() + "}";
            String dtshortkey = "${" + ReservedKeywords.DATASHORTKEY.toString().toLowerCase() + "}";
                    
            for (Element hashEl: hList) {
                HashMap<String, String> attributes = this.getAttributes(hashEl);
                String hashKey = attributes.get(ReservedKeywords.KEY.toString().toLowerCase());
                if(hashKey.isEmpty()) {
                    throw new Exception("Invalid tag in xml, key attribute does not exist for parameters tag");
                }
                String hashValue = attributes.get(ReservedKeywords.VALUE.toString().toLowerCase());
                if(hashValue.isEmpty()) {
                    throw new Exception("Invalid tag in xml, value attribute does not exist for parameters tag");
                }
                    
                String dateValue = getDateValue(hashKey);

                if (hashValue.equals(dt)) {
                    hashValue = hashValue.replace(dt, dateValue);//yyyy-mm-dd
                } else if (hashValue.equals(dtkey)) {
                    hashValue = hashValue.replace(dtkey, dateValue.replace("-", "")); //yyyymmdd 
                } else if (hashValue.equals(dbname)) {
                    hashValue = hashValue.replace(dbname, prop.getProperty(mysql + "." +
                            JobManager.createInstance().env + "." + ReservedKeywords.DBNAME.toString().toLowerCase()));

                 } else if (hashValue.equals(region)) {
                    hashValue = hashValue.replace(region, JobManager.createInstance().region);
                 } else if (hashValue.equals(eq_noteq)) {
                    hashValue = hashValue.replace(eq_noteq, 
                                                JobManager.createInstance().region.equals("int") ? "<>" : "=");
                 } else {
                    if (hashValue.contains(dtshortkey)) {
                       hashValue = hashValue.replace(dtshortkey, 
                                                     dateValue.replace("-", "").substring(2)); //yymmdd
                    }
                }
                parameter.put(hashKey, hashValue);
            }
            par.add(parameter);
        }
    }
    
    private LinkedHashMap getColumns(Element jobEl) throws Exception {
        
        List<Element> pList = getChildList(jobEl, ReservedKeywords.COLUMNS.toString().toLowerCase());
        if (pList.size() > 1) {
            throw new Exception("There should not be more then 1 tag named 'columns'");
        }
        Element columnEl = pList.get(0);
        /*
        <column name="datekey" type="INTEGER"/>
        <column name="keyword" type="VARCHAR(100)"/>
        <column name="URL" type="VARCHAR(600)"/>
        <column name="Pos" type="VARCHAR(100)"/>
        */
        List<Element> hList = getChildList(columnEl, ReservedKeywords.COLUMN.toString().toLowerCase());
        LinkedHashMap columns = new LinkedHashMap();
        for (Element hashEl: hList) {
            HashMap<String, String> attributes = this.getAttributes(hashEl);
            String hashKey = attributes.get(ReservedKeywords.NAME.toString().toLowerCase());
            if(hashKey.isEmpty()) {
                throw new Exception("Invalid tag in xml, name attribute does not exist for column tag");
            }
            String hashValue = attributes.get(ReservedKeywords.TYPE.toString().toLowerCase());
            if(hashValue.isEmpty()) {
                throw new Exception("Invalid tag in xml, type attribute does not exist for column tag");
            }

            columns.put(hashKey, hashValue);
        } 
        return columns;
    }
    
    private String getDateValue(String hashKey) throws Exception{
        String hashValue;
        if (hashKey.equals(ReservedKeywords.END_DT.toString().toLowerCase())) {
            hashValue = JobManager.createInstance().endDate;
        } else {
            hashValue = JobManager.createInstance().date;
        }
        return hashValue == null ? "" : hashValue;
    }
            
    private String addResultSetFields(Element jobEl, List<Object> par, HashMap<String, String> overrides) throws Exception {
        List<Element> pList = getChildList(jobEl, ReservedKeywords.RESULTSET.toString().toLowerCase());
        String resultSetName;
        if (pList.size() != 1) {
            throw new Exception("Invalid number of resultSets in " + 
                    jobEl.getAttribute(ReservedKeywords.NAME.toString().toLowerCase()));
        }
        
        Element parEl = pList.get(0);
        /*
        <resultSet connectionType="mysql" name="glb_performance_reporting">
            <column name="unit" override="unit1"/>
            <column name="unit" override="unit1"/>
        </resultSet>
        */

        HashMap<String, String> attributes = this.getAttributes(parEl);
        resultSetName = attributes.get(ReservedKeywords.NAME.toString().toLowerCase());
        if(resultSetName.isEmpty()) {
            throw new Exception("Invalid tag in xml, name attribute does not exist for resultSet tag");
        }
        String region = "${" + ReservedKeywords.REGION.toString().toLowerCase() + "}";
        resultSetName = resultSetName.replace(region, JobManager.createInstance().region);

        String dbtype = attributes.get(ReservedKeywords.CONNECTIONTYPE.toString().toLowerCase());
        // as we already get the parameters, lets substitude them in dbtype and resultSetName
        dbtype = Utility.substituteParameters(dbtype, (HashMap)par.get(0));
        resultSetName = Utility.substituteParameters(resultSetName, (HashMap)par.get(0));
        if(dbtype.isEmpty()) {
            throw new Exception("Invalid tag in xml, connectionType attribute does not exist for resultset tag");
        }
        par.add(JobRunnerConnection.getConnectionType(dbtype));

        par.add(resultSetName);

        List<Element> hList = getChildList(parEl, ReservedKeywords.COLUMN.toString().toLowerCase());
        
        for (Element hashEl: hList) {
            HashMap<String, String> columnAttributes = this.getAttributes(hashEl);
            String fieldName = columnAttributes.get(ReservedKeywords.NAME.toString().toLowerCase());
            if(fieldName.isEmpty()) {
                throw new Exception("Invalid tag in xml, name attribute does not exist for resultSet tag");
            }
            String override = columnAttributes.get(ReservedKeywords.OVERRIDE.toString().toLowerCase());
            if(fieldName.isEmpty()) {
                throw new Exception("Invalid tag in xml, override attribute does not exist for resultSet tag");
            }

            overrides.put(fieldName, override);
        }
        
        return resultSetName;
    }
    
    private void initKeyFieldNames(Element rootEl) throws Exception {
        List<Element> nList = getChildList(rootEl, ReservedKeywords.RESULTSET.toString().toLowerCase());

        /*
        <ResultSet name="int_reporting" region="int,needish">
            <column type="varchar" name="report_date"/>
            <column type="int" name="report_week"/>
            <column type="int" name="report_month"/>
            <column type="int" name="report_year"/>
            <column type="varchar" name="country"/>          
            <column type="varchar" name="region"/>          
            <column type="varchar" name="economic_area"/>          
            <column type="varchar" name="marketing_channel"/> 
            <column type="varchar" name="sub_channel"/>
        </ResultSet>
        */
        
        String regionToSubstitude = "${"+ReservedKeywords.REGION.toString().toLowerCase()+"}";
        for (Element el: nList) {
            
            HashMap<String, String> attributes = getAttributes(el);
            
            String reg = attributes.get(ReservedKeywords.REGION.toString().toLowerCase());

            if(reg.isEmpty()) {
                throw new Exception("Invalid tag in xml, region attribute does not exist for ResultSet tag");
            }
            
            List<String> regions = Arrays.asList((reg).split(","));
            if (regions.indexOf(JobManager.createInstance().region) == -1) {
                // if the xml job is not created for passed region then ignore this job 
                continue;
            }
            
            String name = attributes.get(ReservedKeywords.NAME.toString().toLowerCase());
            if(name.isEmpty()) {
                throw new Exception("Invalid tag in xml, name attribute does not exist for ResultSet tag");
            }
            
            List<String> fields = new ArrayList();
            List<Element> pList = getChildList(el, ReservedKeywords.COLUMN.toString().toLowerCase());
            
            for (Element pEl: pList) {
                HashMap<String, String> columnAttributes = getAttributes(pEl);
                
                String fieldName = columnAttributes.get(ReservedKeywords.NAME.toString().toLowerCase());
                if(fieldName.isEmpty()) {
                    throw new Exception("Invalid tag in xml, name attribute does not exist for FieldName tag");
                }
                fields.add(fieldName); 
            }
            name = name.replace(regionToSubstitude, JobManager.createInstance().region);
            resultSetParameters.put(name, fields);
        }
    }

    private JobObject.Asserter getJobAsserter(Element el) throws Exception {
        // <asserter datatype="lesser" column="2" condition="greator" value="100000" />
        List<Element> nList = getChildList(el, ReservedKeywords.ASSERTER.toString().toLowerCase());
        
        if (nList.size() > 1) {
            throw new Exception("Duplicate asserter tag in " + el.toString());
        }
        JobObject.Asserter ja = null;
        for (Element parEl: nList) {
            HashMap<String, String> attributes = this.getAttributes(parEl);
            String datatype = attributes.get(ReservedKeywords.DATATYPE.toString().toLowerCase());
            if(datatype.isEmpty()) {
                throw new Exception("Invalid tag in xml, datatype attribute does not exist for ASSERTER tag in " + el.toString());
            }
            if (!datatype.equals(ReservedKeywords.INTEGER.toString().toLowerCase()) &&
                !datatype.equals(ReservedKeywords.VARCHAR.toString().toLowerCase())) {
                throw new Exception("Invalid datatype: " + datatype + ", in " + el.toString());
            }
            
            String column = attributes.get(ReservedKeywords.COLUMN.toString().toLowerCase());
            if(column.isEmpty()) {
                throw new Exception("Invalid tag in xml, column attribute does not exist for ASSERTER tag in " + el.toString());
            }
            
            String condition = attributes.get(ReservedKeywords.CONDITION.toString().toLowerCase());
            if(condition.isEmpty()) {
                throw new Exception("Invalid tag in xml, condition attribute does not exist for ASSERTER tag in " + el.toString());
            }
            if (!condition.equals(ReservedKeywords.LESSER.toString().toLowerCase()) &&
                !condition.equals(ReservedKeywords.EQUAL.toString().toLowerCase()) && 
                !condition.equals(ReservedKeywords.GREATER.toString().toLowerCase())) {
                
                throw new Exception("Invalid condition: " + condition + ", in " + el.toString());
            }
            
            String value = attributes.get(ReservedKeywords.VALUE.toString().toLowerCase());
            if(datatype.isEmpty()) {
                throw new Exception("Invalid tag in xml, value attribute does not exist for ASSERTER tag in " + el.toString());
            }
            
            ja = new JobObject.Asserter(JobObject.AsserterDataType.valueOf(datatype.toUpperCase()), 
                                        JobObject.AsserterCondition.valueOf(condition.toUpperCase()), 
                                        Integer.parseInt(column), 
                                        value);
        }
        return ja;
    }
}
