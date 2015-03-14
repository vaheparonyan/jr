
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.postgresql.util.Base64;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vparonyan
 */
public class BrightEdgeDataCollector extends JobObject{

    private static final String authString = "vparonyan@groupon.com" + ":" + "Group0nDub1";
    private static final String authStringEnc = Base64.encodeBytes(authString.getBytes());
    
    private final HashMap<String, String> IdToDomain = new HashMap();
    private String startDay, endDay, outFile;

    public BrightEdgeDataCollector (List<Object> par, String name, String onError, Asserter asserter){
        super(par, name, onError, asserter);
    }

    private HashMap parameter = null;
    
    private class KeywordData {
        public String text;
        public String week;
        public String country;
        public String url;
        public String searchEngine;
        public String rank;
    }
    
    private final List<KeywordData> keywords = new ArrayList();

    @Override
    public void setProperties(HashMap parameter) {
        this.parameter = parameter;
    }
    
    @Override
    public void execute() throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        if (!JobManager.createInstance().simulate) {

            if ( this.parameter == null) {
                throw new Exception ("The property parameters are not set.");
            }
            // iterate over parameters and get start_dt, end_dt and outputFile, 
            // all the rest are ignored as we are not going to substitude them
            for (Object key: parameter.keySet()) {
                String k = (String)key;
                String v = (String)parameter.get(key);

                switch (k) {
                    case "start_dt":
                        this.startDay = v;
                        break;
                    case "end_dt":
                        this.endDay = v;
                        break;
                    case "outFile":
                        this.outFile = v;
                        break;
                }
            }

            if (startDay == null || endDay == null || outFile == null) {
                throw new Exception("One of the parameters is missing: start_dt | end_dt | outFile");
            }

            getDomains();

            getKeywordRankings();

            dumpKeywordRankings(outFile);
        }
        
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());        
    }
    
    private List<Element> getChildList(Element rootEl, String name) throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        List<Element> nList = new ArrayList<>();
        
        for (Node child = rootEl.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (name.isEmpty() || name.equals(child.getNodeName().toLowerCase())) {
                    nList.add((Element) child);
                }
            }
        }
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());
        return nList;
    }
    
    private void parseDomainXml(String xml) throws ParserConfigurationException, SAXException, IOException, Exception {
        /*
        <domains xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://api.brightedge.com/2.0" xsi:schemaLocation="http://api.brightedge.com/2.0 http://api.brightedge.com/2.0/api.xsd">
            <domain id="23849.3872">groupon.com</domain>
        </domains>
        */
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
        Element rootEl = document.getDocumentElement();
        List<Element> nList = getChildList(rootEl, "domain");
        
        for (Element el: nList) {
            if (el.hasAttribute("id")) {
                IdToDomain.put(el.getAttribute("id"), el.getTextContent());
            } else {
                throw new Exception("Invalid tag: " + el.getTextContent());
            }
        }
        
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());
    }
    
    private void getDomains() throws MalformedURLException, IOException, Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        
        URL url = new URL("https://api.brightedge.com/2.0/domains");
        
        URLConnection urlConnection = url.openConnection();
        
        urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
        InputStream is = urlConnection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);

        int numCharsRead;
        char[] charArray = new char[1024];
        StringBuilder sb = new StringBuilder();
        while ((numCharsRead = isr.read(charArray)) > 0) {
            sb.append(charArray, 0, numCharsRead);
        }

        parseDomainXml(sb.toString());
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());
    }

    private int getKeywordRankings(String u, String domainId, String attr) throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        URL url = new URL(u);

        URLConnection urlConnection = url.openConnection();

        urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
        InputStream is = urlConnection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);

        int numCharsRead;
        char[] charArray = new char[1024];
        StringBuilder sb = new StringBuilder();
        while ((numCharsRead = isr.read(charArray)) > 0) {
            sb.append(charArray, 0, numCharsRead);
        }
            
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());
        return getKeywordRankingsXml(sb.toString(), domainId, attr);
    }
    
    private int getKeywordRankingsXml(String xml, String domainId, String attr) throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        /*
        <?xml version='1.0' encoding='utf-8'?>
        <rankings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://api.brightedge.com/2.0" count="2" total="4206" offset="0" xsi:schemaLocation="http://api.brightedge.com/2.0 http://api.brightedge.com/2.0/api.xsd">
          <ranking keyword="Acupuncture">
            <ranks>
              <rank week="201425" country="us" searchengine="bing">NR</rank>
              <rank week="201425" country="us" searchengine="google">NR</rank>
            </ranks>
          </ranking>
          <ranking keyword="Atlanta">
            <ranks>
              <rank week="201425" country="us" url="www.groupon.com/local/atlanta" searchengine="bing">28</rank>
              <rank week="201425" country="us" url="www.groupon.com/local/atlanta" searchengine="google">66</rank>
            </ranks>
          </ranking>
        </rankings>
        */
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
        Element rootEl = document.getDocumentElement();
        if (!rootEl.hasAttribute("total") ||
            !rootEl.hasAttribute("count") ||
            !rootEl.hasAttribute("offset")) {
            
            throw new Exception("Invalid tag: " + rootEl.getTextContent());
        }
        
        int ret = Integer.parseInt(rootEl.getAttribute(attr));

        if (attr.equals("count")) {
            List<Element> nList = getChildList(rootEl, "ranking");

            for (Element el: nList) {
                if (el.hasAttribute("keyword")) {
                  
                    collectData(el, domainId, el.getAttribute("keyword"));
                    
                } else {
                    throw new Exception("Invalid tag: " + el.getTextContent());
                }
            }
        }
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());

        return ret;
    }
    
    private void collectData(Element rootEl, String domainId, String text) throws Exception {
        List<Element> ranks = getChildList(rootEl, "ranks");

        for (Element el: ranks) {
            
            List<Element> rankList = getChildList(el, "rank");

            for (Element e: rankList) {
                KeywordData k = new KeywordData();
                
                //<rank week="201425" country="us" url="www.groupon.com/local/atlanta" searchengine="bing">28</rank>
                if (!e.hasAttribute("week") ||
                    !e.hasAttribute("country") ||
                    !e.hasAttribute("searchengine")) {
                    
                    throw new Exception("Invalid tag: " + el.getTextContent());
                }
                
                k.week = e.getAttribute("week");
                k.country = e.getAttribute("country");
                k.url = e.getAttribute("url");
                if (!k.url.isEmpty()) {
                    int index = k.url.indexOf(this.IdToDomain.get(domainId)+"/") + this.IdToDomain.get(domainId).length() + 1;
                    if (index != -1) {
                        k.url = k.url.substring(index);
                    }
                }
                k.searchEngine = e.getAttribute("searchengine");
                k.rank = e.getTextContent();
                k.text = text;
                
                this.keywords.add(k);
            }
        }
    }
    
    private void getKeywordRankings() throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());
        //  https://api.brightedge.com/2.0/domain_ranking_keywords/<domain_id>/all?startday=20140615\&endday=20140616\&count=0\&offset=0\&rankmode=2
        
        String url = "https://api.brightedge.com/2.0/domain_ranking_keywords/";
        
        for (String id: this.IdToDomain.keySet()) {
            // geting keywordCount
            String u = url + id + "/all?rankmode=0&startday=" + this.startDay + "&endday="+this.endDay + "&count=0";
            int total = getKeywordRankings(u, id, "total");
            Integer count = 0;
            Integer offset = 0;
            while (offset < total) {
                u = url + id + "/all?startday=" + this.startDay + "&endday="+this.endDay;
                u += "&offset=" + offset.toString();
                
                count = getKeywordRankings(u, id, "count");
                
                offset += count;
            }
        } 
        
        LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());        
    }
     
    private void dumpKeywordRankings(String fileName) throws Exception {
        LoggerHelper.logInfoStart(BrightEdgeDataCollector.class.getName());

        Writer writer = new BufferedWriter(new OutputStreamWriter(
                                 new FileOutputStream(fileName)));
        //header
        //writer.write("keywords\turl\trank\tweek\tsearchengine\tcountry");
        //writer.write("\n");

        for (KeywordData o: this.keywords) {
            //text
            writer.write(o.text);
            writer.write("\t");
            
            //url
            writer.write(o.url);
            writer.write("\t");
           
            //rank
            if (o.rank.equals("NR")) {
                writer.write("200");
            } else {
                writer.write(o.rank);
            }
            writer.write("\t");
            
            //week
            writer.write(o.week);
            writer.write("\t");

            //searchEngine
            writer.write(o.searchEngine);
            writer.write("\t");
           
            //country
            writer.write(o.country);
           
            writer.write("\n");

            writer.flush();
       }
       LoggerHelper.logInfoEnd(BrightEdgeDataCollector.class.getName());
    }
    
}
