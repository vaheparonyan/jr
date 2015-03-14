/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vparonyan
 */

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vparonyan
 */
public class SiteMapUrls extends JobObject {

    //private static final String CUSTOMERTAXONOMY_GUID = "2b90adda-524a-47b5-98de-20906b7a766f";
    private static final String SITEMAP_URL = "http://seo-content-optimizer.snc1/seoadmin/v1/sitemaps";

    
    public SiteMapUrls(List<Object> par, String name, String onError, JobObject.Asserter asserter) {
        super(par, name, onError, asserter);
    }

    private HashMap parameter = null;

    private final Set<String> pageTypes = new HashSet();
    private final Set<String> rowSet = new HashSet();
    
    @Override
    public void setProperties(HashMap parameter) {
        this.parameter = parameter;
    }
    
    //http://seo-content-optimizer.snc1/seoadmin/v1/sitemaps
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(SiteMapUrls.class.getName());
        if (!JobManager.createInstance().simulate) {
                    
            if ( this.parameter == null) {
                throw new Exception ("The parameter properties are not set.");
            }

            readSiteMapTypes();
            readSiteMaps();
            dumpSiteMaps();
        }
        LoggerHelper.logInfoEnd(SiteMapUrls.class.getName());    
    }
    
    private void dumpSiteMaps() throws Exception {
        LoggerHelper.logInfoStart(SiteMapUrls.class.getName());
        String fileName = this.parameter.get("outfile").toString();
        
        PrintWriter writter = new PrintWriter(fileName, "UTF-8");
        
        for (String c: rowSet) {
            writter.write(c);
            writter.write("\n");
        }
        
        writter.close();
        LoggerHelper.logInfoEnd(SiteMapUrls.class.getName());
    }
    
    private void readSiteMaps() throws Exception {
        LoggerHelper.logInfoStart(SiteMapUrls.class.getName());
        
        for (String type : this.pageTypes) {
            URL url = new URL(SITEMAP_URL + "/" + type);

            URLConnection urlConnection = url.openConnection();

            InputStream is = urlConnection.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);

            BufferedReader in = new BufferedReader(isr);
            String line;
            while ((line = in.readLine()) != null) {
                rowSet.add(this.parameter.get("date") + "|||" + line + "|||" + type );
            }
        }
        LoggerHelper.logInfoEnd(SiteMapUrls.class.getName()); 
    }

    private void readSiteMapTypes() throws Exception {
        LoggerHelper.logInfoStart(SiteMapUrls.class.getName());
        
        URL url = new URL(SITEMAP_URL);
        
        URLConnection urlConnection = url.openConnection();

        InputStream is = urlConnection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);

        JSONParser jsonParser = new JSONParser();
        JSONObject sitemapObject = (JSONObject)jsonParser.parse(isr);
        JSONArray jsonObjects = (JSONArray)sitemapObject.get("sitemaps");
       
        // take the elements of the json array
        for(int i=0; i<jsonObjects.size(); i++){
            JSONObject jsonObj = (JSONObject)(jsonObjects.get(i));
            pageTypes.add(getString(jsonObj.get("name")));
        }
 
        LoggerHelper.logInfoEnd(SiteMapUrls.class.getName()); 
    }
    
    private String getString(Object o) {
        return o == null ? "" : o.toString();
    }

}
