
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public class TaxonomyReader extends JobObject {

    //private static final String CUSTOMERTAXONOMY_GUID = "2b90adda-524a-47b5-98de-20906b7a766f";
    private static final String TAXONOMIES_URL = "http://taxonomy-vip.snc1/taxonomies/";
    private static final String FLAT = "/flat";
    
    public TaxonomyReader(List<Object> par, String name, String onError, Asserter asserter) {
        super(par, name, onError, asserter);
    }

    private static class Attribute {
        
        public Attribute() {
            friendlyNamePlural = permalink = seoName = friendlyNameShort = friendlyName = friendlyNameSingular = friendlyNameTitle = "";
        }
        String friendlyNamePlural;
        String permalink;
        String seoName;
        String friendlyNameShort;
        String friendlyName;
        String friendlyNameSingular;
        String friendlyNameTitle;
    }
    
    private static class Locale {
        String categoryName;
        String description;
        String name;
        Attribute attributes;
    }

    private static class Relationship {
        String guid;
        String targetCategoryGuid;
        String sourceCategoryGuid;
        String relationshipTypeFuid;
    }
    
     private static class Category{
        String guid;
        String taxonomyGuid;
        Integer childCount;
        String description;
        
        // mapping locale(es_AR, ....) guid to Locale opbject
        HashMap<String, Locale> locales;
        
        String name;
        String parent;
        List<String> children;
        HashMap<String, String> attributes = new HashMap();

        // mapping relationship guid to Relationship opbject
        HashMap<String, Relationship> relationships;
        Integer depth;
    }
    
    // mapping category guid to Category opbject
    private final List<Category> categories = new ArrayList(); 
    private HashMap parameter = null;
    
    @Override
    public void setProperties(HashMap parameter) {
        this.parameter = parameter;
    }
    
    //http://taxonomy-vip.snc1/taxonomies/2b90adda-524a-47b5-98de-20906b7a766f/flat
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(TaxonomyReader.class.getName());
        if (!JobManager.createInstance().simulate) {
                    
            if ( this.parameter == null) {
                throw new Exception ("The parameter properties are not set.");
            }

            readTaxonomies(parameter);

            String fileName = (String)parameter.get("outfile");
            if (fileName == null || fileName.isEmpty()) {
                fileName = Utility.generateFileName("taxonomies", ".tsv");
            }
            dumpObjects(fileName);
            // tdload to teradata
        }
        LoggerHelper.logInfoEnd(TaxonomyReader.class.getName());    
    }
    
    private void dumpCategoryLocale(Category c, Locale l, Writer writer, String seperator, String head, String tail) throws IOException {
        writer.write(head);
        
        writer.write(l == null ? "en_US" : l.name.substring(0,5));
        writer.write(seperator);

        writer.write(l == null || l.categoryName.isEmpty() ? c.name : l.categoryName);
        writer.write(seperator);

        writer.write(l == null || l.description.isEmpty() ? c.name : l.description);
        writer.write(seperator);

        writer.write(c.guid);
        writer.write(seperator);

        writer.write(c.taxonomyGuid);
        writer.write(seperator);

        writer.write(c.childCount.toString());
        writer.write(seperator);

        writer.write(c.parent);
        writer.write(seperator);

        writer.write(c.depth.toString());
        writer.write(seperator);

        writer.write(l == null ? getString(c.attributes.get("friendly_name_plural")) : l.attributes .friendlyNamePlural);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("permalink")) : l.attributes.permalink);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("seo_name")) : l.attributes.seoName);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("friendly_name_short")) : l.attributes.friendlyNameShort);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("friendly-name")) : l.attributes.friendlyName);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("friendly_name_singular")) : l.attributes.friendlyNameSingular);
        writer.write(seperator);
        writer.write(l == null ? getString(c.attributes.get("friendly_name_title")) : l.attributes.friendlyNameTitle);
        writer.write(seperator);

        writer.write(String.valueOf(c.relationships.size()));

        writer.write(tail);
        
        writer.write("\n");
        
        writer.flush();
    }
    
    private void dumpHeader(Writer writer) throws IOException {
        writer.write("locale");
        writer.write("\t");

        writer.write("categoryName");
        writer.write("\t");

        writer.write("description");
        writer.write("\t");

        writer.write("guid");
        writer.write("\t");

        writer.write("taxonomyGuid");
        writer.write("\t");

        writer.write("childCount");
        writer.write("\t");

        writer.write("parent");
        writer.write("\t");

        writer.write("depth");
        writer.write("\t");

        writer.write("friendlyNamePlural");
        writer.write("\t");
        writer.write("permalink");
        writer.write("\t");
        writer.write("seoName");
        writer.write("\t");
        writer.write("friendlyNameShort");
        writer.write("\t");
        writer.write("friendlyName");
        writer.write("\t");
        writer.write("friendlyNameSingular");
        writer.write("\t");
        writer.write("friendlyNameTitle");
        writer.write("\t");

        writer.write("relationshipsSize");
        writer.write("\t");

        writer.write("\n");    
    }
    
    private void dumpObjects(String fileName) throws Exception {
        LoggerHelper.logInfoStart(TaxonomyReader.class.getName());
        PrintWriter writter = new PrintWriter(fileName, "UTF-8");
        
        for (Category c: this.categories) {
            dumpCategoryLocale(c, null, writter, "\t", "", "");

            if (c.locales != null) {
                for (Locale l: c.locales.values()) {
                    dumpCategoryLocale(c, l, writter, "\t", "", "");
                }
            }
        }
        
        writter.close();
        /*
        PrintWriter writterIns = new PrintWriter(fileName+"ins.txt", "UTF-8");
        
        for (Category c: this.categories) {
            dumpCategoryLocale(c, null, writterIns, "','", "insert into sandbox.kb_taxonomies values('" , "');");


            if (c.locales != null) {
                for (Locale l: c.locales.values()) {
                    dumpCategoryLocale(c, l, writterIns, "','", "insert into sandbox.kb_taxonomies values('" , "');");
                }
            }
        }
        
        writterIns.close();
        */
        
        LoggerHelper.logInfoEnd(TaxonomyReader.class.getName());
    }
    
    private void readTaxonomies(HashMap parameter) throws Exception {
        LoggerHelper.logInfoStart(TaxonomyReader.class.getName());        
        
        String categoryGuid = (String)parameter.get("taxonomy_guid");
        
        URL url = new URL(TAXONOMIES_URL + categoryGuid + FLAT);
        
        URLConnection urlConnection = url.openConnection();

        InputStream is = urlConnection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);

        JSONParser jsonParser = new JSONParser();
        JSONArray jsonObjects = (JSONArray)jsonParser.parse(isr);
       
        // take the elements of the json array
        for(int i=0; i<jsonObjects.size(); i++){
            Category c = new Category();
            
            JSONObject jsonObj = (JSONObject)(jsonObjects.get(i));
            
            c.guid = getString(jsonObj.get("guid"));
            c.taxonomyGuid = getString(jsonObj.get("taxonomy_guid"));
            c.childCount = Integer.parseInt(getString(jsonObj.get("child_count")));
            c.description = getString(jsonObj.get("description"));
            c.name = getString(jsonObj.get("name"));
            
            
            c.locales = new HashMap();
            getCategoryLocales(c, jsonObj);
            
            c.parent = getString(jsonObj.get("parent"));
        
            c.children = new ArrayList();
            getCategoryChildren(c, jsonObj);
            
            getCategoryAttributes(c.attributes, jsonObj);
            
            //HashMap<String, Relationship> relationships;
            c.relationships = new HashMap();
            getRelationships(c.relationships, jsonObj);
            
            c.depth = Integer.parseInt(getString(jsonObj.get("depth")));        
            
            categories.add(c);
        }
 
        LoggerHelper.logInfoEnd(TaxonomyReader.class.getName());    
    }
     
    private void getRelationships(HashMap c, JSONObject jsonObj) {
        JSONArray ar = (JSONArray)(jsonObj.get("attributes"));
        for (int i = 0; i < ar.size(); i++) {
            JSONObject obj = (JSONObject)(ar.get(i));
            Relationship r = new Relationship();
            r.guid = getString(obj.get("guid"));
            r.targetCategoryGuid = getString(obj.get("target_category_guid"));
            r.sourceCategoryGuid = getString(obj.get("source_category_guid"));
            r.relationshipTypeFuid = getString(obj.get("relationship_type_guid"));
            
            c.put(r.guid, r);
        }
    }

    private void getCategoryLocales(Category c, JSONObject jsonObj) {
        JSONArray jsonLocales = (JSONArray)(jsonObj.get("locales"));
        for (int l = 0; l < jsonLocales.size(); l++) {
            JSONObject lObj = (JSONObject)(jsonLocales.get(l));
            Locale loc = new Locale();
            loc.name = getString(lObj.get("name"));
            loc.description = getString(lObj.get("description"));
            loc.categoryName = getString(lObj.get("category_name"));

            loc.attributes = new Attribute();

            getLocaleAttributes(loc.attributes, lObj);
            
            c.locales.put(loc.name, loc);
        }
    }
    
    private void getCategoryChildren(Category c, JSONObject jsonObj) {
        JSONArray jsonChildren = ((JSONArray)(jsonObj.get("children")));
        for (int j = 0; j < jsonChildren.size(); j++) {
            c.children.add(getString(jsonChildren.get(j)));
        }
    }
    
    private void getLocaleAttributes(Attribute a, JSONObject jsonObj) {
        JSONObject at = (JSONObject)(jsonObj.get("attributes"));
        if (at != null) {
            a.friendlyNamePlural = getString(at.get("friendly_name_plural"));
            a.permalink = getString(at.get("permalink"));
            a.seoName = getString(at.get("seo_name"));
            a.friendlyNameShort = getString(at.get("friendly_name_short"));
            a.friendlyName = getString(at.get("friendly-name"));
            a.friendlyNameSingular = getString(at.get("friendly_name_singular"));
            a.friendlyNameTitle = getString(at.get("friendly_name_title"));
        }
    }
    
    private void getCategoryAttributes(HashMap<String, String> a, JSONObject jsonObj) throws Exception {
        JSONArray at = (JSONArray)(jsonObj.get("attributes"));
        for (int i = 0; i < at.size(); i++) {
            JSONObject obj = (JSONObject)(at.get(i));
            for (Object k: obj.keySet()) {
                a.put(k.toString(), getString(obj.get(k)));
            }
        }
    }
    
    private String getString(Object o) {
        return o == null ? "" : o.toString();
    }

}
