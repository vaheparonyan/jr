import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.v201406.cm.Language;
import com.google.api.ads.adwords.axis.v201406.cm.Location;
import com.google.api.ads.adwords.axis.v201406.cm.NetworkSetting;
import com.google.api.ads.adwords.axis.v201406.cm.Paging;
import com.google.api.ads.adwords.axis.v201406.o.Attribute;
import com.google.api.ads.adwords.axis.v201406.o.AttributeType;
import com.google.api.ads.adwords.axis.v201406.o.IdeaType;
import com.google.api.ads.adwords.axis.v201406.o.MoneyAttribute;
import com.google.api.ads.adwords.axis.v201406.o.DoubleAttribute;
import com.google.api.ads.adwords.axis.v201406.o.LanguageSearchParameter;
import com.google.api.ads.adwords.axis.v201406.o.LocationSearchParameter;
import com.google.api.ads.adwords.axis.v201406.o.NetworkSearchParameter;
import com.google.api.ads.adwords.axis.v201406.o.LongAttribute;
import com.google.api.ads.adwords.axis.v201406.o.RelatedToQuerySearchParameter;
import com.google.api.ads.adwords.axis.v201406.o.RequestType;
import com.google.api.ads.adwords.axis.v201406.o.SearchParameter;
import com.google.api.ads.adwords.axis.v201406.o.StringAttribute;
import com.google.api.ads.adwords.axis.v201406.o.TargetingIdea;
import com.google.api.ads.adwords.axis.v201406.o.TargetingIdeaPage;
import com.google.api.ads.adwords.axis.v201406.o.TargetingIdeaSelector;
import com.google.api.ads.adwords.axis.v201406.o.TargetingIdeaServiceInterface;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.conf.ConfigurationLoadException;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.ads.common.lib.utils.Maps;
import com.google.api.client.auth.oauth2.Credential;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import static java.lang.Math.min;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;

// @todo : Vahe, the type should not be part of parameters, it must be an attribute for job tag
public class GoogleAdKeywordStatsJobObject extends JobObject {
    
    public static enum Type {STATS, IDEAS};
    
    private String inputFile = null;
    private String outputFile = null;
    private String startDay = null, endDay = null;
    private HashMap parameters = null;
    private Credential oAuth2Credential = null;
    private AdWordsSession session = null;
    private AdWordsServices adWordsServices = null;
    Type type = Type.STATS;
    
    public GoogleAdKeywordStatsJobObject(List<Object> par, String name, String onError, Asserter asserter){
        super(par, name, onError, asserter);
    }
   
    @Override
    public void setProperties(HashMap parameters) throws ConfigurationLoadException, ValidationException, OAuthException, Exception {
        this.parameters = parameters;
        
        if ( this.parameters == null) {
            throw new Exception ("The property parameters are not set.");
        }
        // iterate over parameters and get start_dt, end_dt and outputFile, 
        // all the rest are ignored as we are not going to substitude them
        for (Object key: parameters.keySet()) {
            String k = (String)key;
            String v = (String)parameters.get(key);

            switch (k) {
                case "infile":
                    this.inputFile = v;
                    break;
                case "outfile":
                    this.outputFile = v;
                    break;
                case "start_dt":
                    this.startDay = v;
                    break;
                case "end_dt":
                    this.endDay = v;
                    break;
                case "type":
                    this.type = Type.valueOf(v);
                    break;
            }
        }
            
        if ( this.inputFile == null || this.outputFile == null || this.startDay == null || this.endDay == null) {
           throw new Exception ("Not all property parameters are set.");
        }
        
        // Generate a refreshable OAuth2 credential similar to a ClientLogin token
        // and can be used in place of a service account.
        oAuth2Credential = new OfflineCredentials.Builder()
            .forApi(Api.ADWORDS)
            .fromFile()
            .build()
            .generateCredential();
        
        session = new AdWordsSession.Builder()
            .fromFile()
            .withOAuth2Credential(oAuth2Credential)
            .build();
        
        // Construct an AdWordsSession.
        adWordsServices = new AdWordsServices();
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(GoogleAdKeywordStatsJobObject.class.getName());
        
        HashMap<String, LinkedList> localQueries = new HashMap();
        
        getQueries(localQueries);
        
        PrintWriter writer = new PrintWriter(this.outputFile, "UTF-8");
        for (String key : localQueries.keySet()) {
            String[] queries = (String[])localQueries.get(key).toArray(new String[localQueries.get(key).size()]);
            String locale = (String)key;

            if (this.type == Type.STATS) {
                getStats(locale, queries, writer);
            } else if (this.type == Type.IDEAS) {
                getIdeas(locale, queries, writer);
            }
        }
        
        writer.close();        
        LoggerHelper.logInfoEnd(GoogleAdKeywordStatsJobObject.class.getName());
    }
    
    private void getStats(String locale, String[] queries, PrintWriter writer) throws Exception {
       
        HashSet<String> querySet = new HashSet();
        querySet.addAll(Arrays.asList(queries));
         
        int startIndex = 0;
        
        // Get the TargetingIdeaService.
        TargetingIdeaServiceInterface targetingIdeaService =
            adWordsServices.get(session, TargetingIdeaServiceInterface.class);

        // Create selector.
        TargetingIdeaSelector selector = new TargetingIdeaSelector();
        selector.setRequestType(RequestType.STATS);
        selector.setIdeaType(IdeaType.KEYWORD);
        selector.setRequestedAttributeTypes(new AttributeType[] {
            AttributeType.KEYWORD_TEXT,
            AttributeType.SEARCH_VOLUME,
            AttributeType.AVERAGE_CPC,
            AttributeType.COMPETITION});

        // Set selector paging (required for targeting idea service).
        Paging paging = new Paging();
        paging.setStartIndex(startIndex);
        paging.setNumberResults(1000);
        selector.setPaging(paging);

        // Create related to query search parameter.
        RelatedToQuerySearchParameter relatedToQuerySearchParameter =
            new RelatedToQuerySearchParameter();


        // get location and language from locale
        LocationSearchParameter locationSearchParameter = new LocationSearchParameter();
        Location location = new Location();

        LanguageSearchParameter languageParameter = new LanguageSearchParameter();
        Language lang = new Language();

        getLocationAndLanguage(locale, lang, location);

        locationSearchParameter.setLocations(new Location[] { location });
        languageParameter.setLanguages(new Language[] {lang});
        
        NetworkSearchParameter networkSearchParameter = new NetworkSearchParameter();
        NetworkSetting networkSetting = getNetworkSetting();
        networkSearchParameter.setNetworkSetting(networkSetting);
        
        while (!querySet.isEmpty()) {
            List<String> qs = new ArrayList();
            qs.addAll(querySet);
            int count = min(500, qs.size());
            qs = qs.subList(0, count);

            relatedToQuerySearchParameter.setQueries(qs.toArray(new String[qs.size()]));

            selector.setSearchParameters(
                new SearchParameter[] { relatedToQuerySearchParameter, 
                                        languageParameter, 
                                        locationSearchParameter,
                                        networkSearchParameter});

            selector.setLocaleCode(locale.substring(locale.length() - 2));

            while (count > 0) {
                //changed = false;
                // Get related keywords.
                TargetingIdeaPage page = targetingIdeaService.get(selector);

                System.out.println("There are " + Integer.toString(querySet.size()) + " keywords to handle....");
                // Display related keywords.
                if (page.getEntries() != null && page.getEntries().length > 0) {
                    for (TargetingIdea targetingIdea : page.getEntries()) {
                        Map<AttributeType, Attribute> data = Maps.toMap(targetingIdea.getData());
                        String keyword = (data.get(AttributeType.KEYWORD_TEXT) == null) ? "" : ((StringAttribute)data.get(AttributeType.KEYWORD_TEXT)).getValue();

                        //get search volume
                        LongAttribute searchVolumeAttr = ((LongAttribute) data.get(AttributeType.SEARCH_VOLUME));
                        Long searchVolume = searchVolumeAttr == null ? 0 : searchVolumeAttr.getValue() == null ? 0 : searchVolumeAttr.getValue().longValue();

                        // get cpc
                        MoneyAttribute cpcAttr = ((MoneyAttribute) data.get(AttributeType.AVERAGE_CPC));
                        Double cpc = (cpcAttr == null || cpcAttr.getValue() == null) ? 0 : Math.round(cpcAttr.getValue().getMicroAmount() / 10000.0)/100.0;

                        // get competition
                        DoubleAttribute competitionAttr = (DoubleAttribute) data.get(AttributeType.COMPETITION);
                        Double competition = (competitionAttr == null || competitionAttr.getValue() == null) ? 0 : Math.round(competitionAttr.getValue().doubleValue()*100.0)/100.0;
                            
                        if (querySet.contains(keyword.toLowerCase())) {
                            
                            count--;
                            querySet.remove(keyword.toLowerCase());
                            qs.remove(keyword);
                            
                            // dump data into file
                            writer.println(locale + "\t" + 
                                           keyword + "\t" + 
                                           searchVolume + "\t" + 
                                           cpc + "\t" +
                                           competition);
                        }
                    }
                } else {
                    System.out.println("No related keywords were found.");
                }
                for (String q: qs) {
                    count--;
                    querySet.remove(q.toLowerCase());
                    writer.println(locale + "\t" + 
                                    q + "\t" + 
                                    "0" + "\t" + 
                                    "0" + "\t" +
                                    "0");
                }
            }
        }
    }
    
    private NetworkSetting getNetworkSetting() {
        NetworkSetting networkSetting = new NetworkSetting();
        
        networkSetting.setTargetGoogleSearch(true);
        networkSetting.setTargetSearchNetwork(false);
        networkSetting.setTargetContentNetwork(false);
        networkSetting.setTargetPartnerSearchNetwork(false);
        
        return networkSetting;
    }
    
    private void getIdeas(String locale, String[] queries, PrintWriter writer) throws Exception {
       
        HashMap<String, HashSet<String>> queriesMap;
        queriesMap = new HashMap();
        for (String q: queries){
            queriesMap.put(q, new HashSet());
        }
         
        int startIndex = 0;
        
        // Get the TargetingIdeaService.
        TargetingIdeaServiceInterface targetingIdeaService =
            adWordsServices.get(session, TargetingIdeaServiceInterface.class);

        // Create selector.
        TargetingIdeaSelector selector = new TargetingIdeaSelector();
        selector.setRequestType(RequestType.IDEAS);
        selector.setIdeaType(IdeaType.KEYWORD);
        selector.setRequestedAttributeTypes(new AttributeType[] {
            AttributeType.KEYWORD_TEXT,
            AttributeType.SEARCH_VOLUME,
            AttributeType.AVERAGE_CPC,
            AttributeType.COMPETITION});

        // Set selector paging (required for targeting idea service).
        Paging paging = new Paging();
        paging.setStartIndex(startIndex);
        paging.setNumberResults(800);
        selector.setPaging(paging);

        // Create related to query search parameter.
        RelatedToQuerySearchParameter relatedToQuerySearchParameter =
            new RelatedToQuerySearchParameter();


        // get location and language from locale
        LocationSearchParameter locationSearchParameter = new LocationSearchParameter();
        Location location = new Location();

        LanguageSearchParameter languageParameter = new LanguageSearchParameter();
        Language lang = new Language();
        
        getLocationAndLanguage(locale, lang, location);

        NetworkSearchParameter networkSearchParameter = new NetworkSearchParameter();
        
        NetworkSetting networkSetting = getNetworkSetting();
        networkSearchParameter.setNetworkSetting(networkSetting);
        
        locationSearchParameter.setLocations(new Location[] { location });
        languageParameter.setLanguages(new Language[] {lang});

        for (String query: queriesMap.keySet()) {
            
            relatedToQuerySearchParameter.setQueries(new String[] {query});
            selector.setSearchParameters(
                new SearchParameter[] { relatedToQuerySearchParameter, 
                                        languageParameter, 
                                        locationSearchParameter,
                                        networkSearchParameter});
            
            selector.setLocaleCode(locale.substring(locale.length() - 2));
            
            TargetingIdeaPage page = targetingIdeaService.get(selector);
            
            if (page.getEntries() != null && page.getEntries().length > 0) {
                for (TargetingIdea targetingIdea : page.getEntries()) {
                    
                    Map<AttributeType, Attribute> data = Maps.toMap(targetingIdea.getData());
                    String keyword = (data.get(AttributeType.KEYWORD_TEXT) == null) ? "" : ((StringAttribute)data.get(AttributeType.KEYWORD_TEXT)).getValue();

                    //get search volume
                    LongAttribute searchVolumeAttr = ((LongAttribute) data.get(AttributeType.SEARCH_VOLUME));
                    Long searchVolume = searchVolumeAttr == null ? 0 : searchVolumeAttr.getValue() == null ? 0 : searchVolumeAttr.getValue().longValue();

                    // get cpc
                    MoneyAttribute cpcAttr = ((MoneyAttribute) data.get(AttributeType.AVERAGE_CPC));
                    Double cpc = (cpcAttr == null || cpcAttr.getValue() == null) ? 0 : Math.round(cpcAttr.getValue().getMicroAmount() / 10000.0)/100.0;

                    // get competition
                    DoubleAttribute competitionAttr = (DoubleAttribute) data.get(AttributeType.COMPETITION);
                    Double competition = (competitionAttr == null || competitionAttr.getValue() == null) ? 0 : Math.round(competitionAttr.getValue().doubleValue()*100.0)/100.0;
        
                    // dump data into file
                    writer.println(locale + "\t" + 
                                   query + "\t" + //original keyword
                                   keyword + "\t" + //suggested keywords
                                   searchVolume + "\t" + 
                                   cpc + "\t" +
                                   competition);
                }
            } else {
                System.out.println("No related keywords were found.");
            }  
        }
    }

    private static void getLocationAndLanguage(String locale, Language lang, Location location) {
        switch (locale) {
            case "en_US":
                location.setId(2840L);
                lang.setId(1000L);
                break;
            case "en_GB":
                location.setId(2826L);
                lang.setId(1000L);
                break;
            case "en_IE":
                location.setId(2372L);  
                lang.setId(1000L);
                break;
            case "de_DE":
                location.setId(2276L);  
                lang.setId(1001L);
                break;
            case "es_ES":
                location.setId(2724L);  
                lang.setId(1003L);
                break;
            case "fr_FR":
                location.setId(2250L);  
                lang.setId(1002L);
                break;
            case "it_IT":
                location.setId(2380L);  
                lang.setId(1004L);
                break;
            case "en_CA":
                location.setId(2124L);  
                lang.setId(1000L);
                break;
            case "fr_CA":
                location.setId(2124L);  
                lang.setId(1002L);
                break;
            default:
                throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    private void getQueries(HashMap localQueries) throws FileNotFoundException, IOException {
        localQueries.clear();
        
        FileInputStream fr = new FileInputStream(this.inputFile);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(fr,"UTF-8"))) {
            String line;
            Integer l = 0;
            while ((line = in.readLine()) != null) {
                String parts[] = line.split("\t");
                if (parts.length == 2) {
                    //System.out.println(parts[0] == null ? "null" :  parts[0] + " - " + parts[1] == null ? "null" :  parts[1]);
                    if (!localQueries.containsKey(parts[0])) {
                        localQueries.put(parts[0], new LinkedList<String>());
                    }
                    String v = parts[1].replaceAll("^\"|\"$", "");
                    ((LinkedList)localQueries.get(parts[0])).add(v.toLowerCase());
                } else {
                    System.out.println("Excluding line #" + l.toString());
                }
                
                l++;
            }
        }
    }

}