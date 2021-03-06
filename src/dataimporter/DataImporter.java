package dataimporter;
import java.sql.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import static java.security.AccessController.getContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.swing.JOptionPane;
import uk.ac.shef.wit.simmetrics.similaritymetrics.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import javax.net.ssl.HttpsURLConnection;



/*
 *
 * @author Katie
 */
public class DataImporter {
    
    static String filename = "C://Users//Katie//Documents//College//FYP//data//glossary_italian_english_conflict_terrorism_1.csv";
    static Connection con = null; 
    static BufferedReader br = null;
    static PreparedStatement pst = null; 
    static ResultSet rs = null;
    
    
    static LookupTable _languagehtc = null;
    static LookupTable _categorieshtc = null;
    static LookupTable _contexthtc = null;
    static LookupTable _synonymhtc = null;
    
    public static String[] parts = null;
    public static String[] categories = null;

    public static String[]  src_contexts = null;
    public static String[]  targ_contexts = null;
 
    
    static final int CATEGORY = 0;
    static final int SRC_TERM = 1;
    static final int SRC_LANG = 2;
    static final int SRC_CONTEXT = 3;
    static final int TARG_LANG = 4;
    static final int TARG_TERM = 5;
    static final int TARG_CONTEXT = 6;
    
    
    static int k=1;
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException, IOException, Exception {
        
        // initalised fails e.g. file doesn't exist
        if(!init()){
            System.out.println("init failed");
            return;
        }
        
        //initalised
        //loop
        String line;

        _languagehtc.dump();
        _categorieshtc.dump();
        _contexthtc.dump();
        _synonymhtc.dump();
        
        
        
        long src_lang_id=0;
        long targ_lang_id=0;
        long src_term_id=0;
        long targ_term_id=0;
        long translation_id=0;
        long sim_translation_id=0;
        long count_query=0;
        
       
        
       
        while((line=br.readLine())!=null)
        {
            //rollbacks before parse reset state on error
            
            
            // line parsed
            if(parsed(line)){
                
                //would start transaction here
                
              
                // call getID method from LookupTable class
                src_lang_id =_languagehtc.getID(parts[SRC_LANG]);
                targ_lang_id =_languagehtc.getID(parts[TARG_LANG]);
                
                
                // call lookup method from TermLookuper class
                src_term_id = Lookuper.Termlookup(con, parts[SRC_TERM], src_lang_id);
                targ_term_id = Lookuper.Termlookup(con, parts[TARG_TERM], targ_lang_id);
                
               
                //synonyms
                DataImporter d = new DataImporter();
                JSONParser parser = new JSONParser();
                Object obj;
                
                if(checkEnglishTerm(src_lang_id)){
                    String api_call = d.sendGet(parts[SRC_TERM]);
                    if(api_call!=null){
                    try{
                        obj = parser.parse(api_call);
                    }
                    catch (ParseException parse) {
                        // Invalid syntax
                        return;
                    }
                    d.insertResults(obj,src_term_id);
                    }
                }
                
                
                if(checkEnglishTerm(targ_lang_id)){
                    String api_call = d.sendGet(parts[TARG_TERM]);
                    if(api_call!=null){
                    //String api_call = d.sendGet("love");
                    try{
                        obj = parser.parse(api_call);
                    }
                    catch (ParseException parse) {
                        // Invalid syntax
                        return;
                    }
                    d.insertResults(obj,targ_term_id);
                    }
                }
                
                        
                try{
                    for(int j=0; j<categories.length; j++)
                    {
                       System.out.println("Inside categories "+categories[j]);
                       long category_id=_categorieshtc.getID(categories[j]);
                       System.out.println("\nCategory id is:"+ category_id);
                       long src_term_has_cat_id = Lookuper.Categorylookup(con, src_term_id,category_id );
                       long targ_term_has_cat_id = Lookuper.Categorylookup(con, targ_term_id, category_id);
                       System.out.println("\nsrc_term_has_cat_id : "+src_term_has_cat_id );
                       System.out.println("\ntarg_term_has_cat_id : "+targ_term_has_cat_id );
                    }
                }
                 catch(Exception e){
                     System.err.println("Categories warning:"+e.getMessage()); 
                 }
                
                try{
                    for(int j=0; j<src_contexts.length; j++)
                    {
                       System.out.println("Inside src context "+src_contexts[j]);
                       System.out.println("src lang id "+src_lang_id);
                       long src_context_id=_contexthtc.getID(src_contexts[j],src_lang_id);
                       System.out.println("\n src context id is:"+ src_context_id);
                       long src_term_has_contetx_id = Lookuper.Contextlookup(con, src_term_id,src_context_id );
                       System.out.println("\nsrc_term_has_context_id : "+src_term_has_contetx_id );
                    }
                }
                catch(Exception e){
                     System.err.println("Source context warning:"+e.getMessage()); 
                 }
                 
                 
                try{
                    for(int m=0; m<targ_contexts.length; m++)
                    {
                       System.out.println("Inside targ contetx "+targ_contexts[m]);
                       System.out.println("targlang id "+targ_lang_id);
                       long targ_context_id= _contexthtc.getID(targ_contexts[m], targ_lang_id);
                       System.out.println("\ntarg context id is:"+ targ_context_id);
                       long targ_term_has_context_id = Lookuper.Contextlookup(con, targ_term_id, targ_context_id);
                       System.out.println("\ntarg_term_has_context_id : "+targ_term_has_context_id );
                    }
                }
                catch(Exception e){
                    System.err.println("Target context warning:"+e.getMessage()); 
                }
                
                /*select count(*) from translations where (src_term_id = 3 and targ_term_id = 4) or (src_term_id = 4 and targ_term_id = 3);
                select count(*) from translations where (src_term_id = src_term_id and targ_term_id = targ_term_id) or (src_term_id = targ_term_id and targ_term_id = src_term_id);
                */
                
                //if greater than 0 dont insert as translation is two way e.g 1 to 2 is same as 2 to 1
                pst = con.prepareStatement("select count(*) from translations where (src_term_id = ? and targ_term_id = ?) or (src_term_id = ? and targ_term_id = ?);");
                        pst.setLong(1, src_term_id);
                        pst.setLong(2, targ_term_id);
                        pst.setLong(3, targ_term_id);
                        pst.setLong(4, src_term_id);
                        rs = pst.executeQuery();
                
                if (rs.next()) {                        
                        count_query = rs.getInt(1);
                        System.out.println("Count query is!! "+count_query);
                }
                
                if(count_query<=0){
                    translation_id = Lookuper.Translationslookup(con, src_term_id, targ_term_id);
                }
                
                
                
                
                // Similarity
                // after every insert to translations inspect all data in translation table
                // select all from translation where src_term_ids are the same
                // & where targ_term_ids are the same
                // next check if they point to same language
               
               // righthand side similar values in table
                try{
                    
                    pst = con.prepareStatement("Select targ_term_id from translations where src_term_id = ?");
                    pst.setLong(1, src_term_id);
                    rs = pst.executeQuery();
                    int count=1;
                    List<Long> targ_values = new ArrayList<>();
                    List<String> similar_terms = new ArrayList<>();
                    
                    while(rs.next()){
                        long targ_trans_id = rs.getLong("targ_term_id");
                        targ_values.add(targ_trans_id);
                        System.out.println("\n!!!!    Translation src ids: "+targ_trans_id+" | count: "+count);
                        count++;                      
                        
                    }
                         
                    long size = targ_values.size();
                    System.out.println("Size is!!: "+size);
                    //always comapare fist with last??
                    if(targ_values.size()>1){
                    
                        long targ_trans_value_1 = targ_values.get(0);
                        System.out.println("Target values 1 is " +targ_trans_value_1);
                        // get last element in list
                        long targ_trans_value_2 = targ_values.get(targ_values.size()-1);
                        System.out.println("Target values 2 is " +targ_trans_value_2);


                        //select id and string
                        pst = con.prepareStatement("Select language_id, term from terms where id in (?,?)");
                        pst.setLong(1, targ_trans_value_1);
                        pst.setLong(2, targ_trans_value_2);
                        rs = pst.executeQuery();

                        //clear list for resue
                        targ_values.clear();

                        while(rs.next()){
                            
                            long lang_trans_id = rs.getLong("language_id");
                            String sim_term = rs.getString("term");
                            System.out.println("term is : >>"+sim_term);
                            targ_values.add(lang_trans_id);
                            similar_terms.add(sim_term);

                        }

                        long lang_trans_value_1 = targ_values.get(0);
                        // get last element in list
                        long lang_trans_value_2 = targ_values.get(targ_values.size()-1);

                        System.out.println("*******language 1 is " +lang_trans_value_1);
                        System.out.println("*******language 2 is " +lang_trans_value_2);


                        String sim_term_1 = similar_terms.get(0);
                        // get last element in list
                        String sim_term_2 = similar_terms.get(similar_terms.size()-1);

                        System.out.println("*******term 1 is " +sim_term_1);
                        System.out.println("*******term 2 is " +sim_term_2);

                        // if equal then calculate similarity score
                        if(lang_trans_value_1 == lang_trans_value_2)
                        {
                            System.out.println("\n\nWhoop they are the same!!");

                            AbstractStringMetric metric = new Levenshtein();
                            float sim_score = metric.getSimilarity(sim_term_1, sim_term_2);
                            System.out.println("Similarity score is " +sim_score);

                            //SimTranslationslookup           
                            sim_translation_id = Lookuper.SimTranslationslookup(con, targ_trans_value_1, targ_trans_value_2,sim_score);
                        }
                    }
                    
                    //clear list for resue
                    targ_values.clear();   
                    
                }
                catch(Exception e){
                    System.err.println("Translation simimarity right handside warning: "+e.getMessage()); 
                }
               
               // righthand side similar values in table
               try{
                    
                    pst = con.prepareStatement("Select src_term_id from translations where targ_term_id = ?");
                    pst.setLong(1, targ_term_id);
                    rs = pst.executeQuery();
                    int count=1;
                    List<Long> src_values = new ArrayList<>();
                    List<String> similar_terms = new ArrayList<>();
                    
                    while(rs.next()){
                        long src_trans_id = rs.getLong("src_term_id");
                        src_values.add(src_trans_id);
                        System.out.println("\n!!!!    Translation src ids: "+src_trans_id+" | count: "+count);
                        count++;                      
                        
                    }
                         
                    long size = src_values.size();
                    System.out.println("Size is!!: "+size);
                    //always comapare fist with last??
                    if(src_values.size()>1){
                    
                        long src_trans_value_1 = src_values.get(0);
                        System.out.println("Target values 1 is " +src_trans_value_1);
                        // get last element in list
                        long src_trans_value_2 = src_values.get(src_values.size()-1);
                        System.out.println("Target values 2 is " +src_trans_value_2);


                        //select id and string
                        pst = con.prepareStatement("Select language_id, term from terms where id in (?,?)");
                        pst.setLong(1, src_trans_value_1);
                        pst.setLong(2, src_trans_value_2);
                        rs = pst.executeQuery();

                        //clear list for resue
                        src_values.clear();

                        while(rs.next()){
                            
                            long lang_trans_id = rs.getLong("language_id");
                            String sim_term = rs.getString("term");
                            System.out.println("term is : >>"+sim_term);
                            src_values.add(lang_trans_id);
                            similar_terms.add(sim_term);

                        }

                        long lang_trans_value_1 = src_values.get(0);
                        // get last element in list
                        long lang_trans_value_2 = src_values.get(src_values.size()-1);

                        System.out.println("*******language 1 is " +lang_trans_value_1);
                        System.out.println("*******language 2 is " +lang_trans_value_2);


                        String sim_term_1 = similar_terms.get(0);
                        // get last element in list
                        String sim_term_2 = similar_terms.get(similar_terms.size()-1);

                        System.out.println("*******term 1 is " +sim_term_1);
                        System.out.println("*******term 2 is " +sim_term_2);

                        // if equal then calculate similarity score
                        if(lang_trans_value_1 == lang_trans_value_2)
                        {
                            System.out.println("\n\nWhoop they are the same!!");

                            AbstractStringMetric metric = new Levenshtein();
                            float sim_score = metric.getSimilarity(sim_term_1, sim_term_2);
                            System.out.println("Similarity score is " +sim_score);

                            //SimTranslationslookup           
                            sim_translation_id = Lookuper.SimTranslationslookup(con, src_trans_value_1, src_trans_value_2,sim_score);
                        }
                    }
                    
                    //clear list for resue
                    src_values.clear();   
                    
                }
                catch(Exception e){
                    System.err.println("Translation simimarity left handside warning: "+e.getMessage()); 
                }
            }
            
            System.out.println("> "+line);
        }
    }
    
    public static boolean init() throws ClassNotFoundException, SQLException, FileNotFoundException{
        
        File f = new File(filename);
        if(f.exists()){

            Class.forName("com.mysql.jdbc.Driver");
        
            con = DriverManager.getConnection("jdbc:mysql://danu6.it.nuigalway.ie:3306/mydb1803","mydb1803gk","ki1riw");
            br = new BufferedReader(new FileReader(filename));
            
            //load from db
            _languagehtc =new LookupTable(con, 
                "Insert into languages (language) VALUES (?);", 
                "Select id from languages where language=? limit 1;",
                "Select language, id from languages;"
            );
             
            _categorieshtc =new LookupTable(con, 
                "Insert into categories (category) VALUES (?);", 
                "Select id from categories where categories=? limit 1;",
                "Select category,id from categories;"
            );
            
            //building on fly dont' have to worry about loading
            //since context is language driven we don't know which language to load
            _contexthtc =new LookupTable(con, 
                "Insert into context (context,language_id) VALUES (?,?);", 
                "Select id from context where context=? and language_id=? limit 1;",
                null
            ); 
            
            
            _synonymhtc =new LookupTable(con, 
                "Insert into synonyms (synonym) VALUES (?);", 
                "Select id from synonyms where synonyms=? limit 1;",
                "Select synonym,id from synonyms;"
            );
                    
            return true;
        }

        return false;
    }
      

    public static boolean parsed(String line){
      
       parts = null;
       // keep commas within column
       parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",7);
       
        for(int i=0; i<parts.length; i++)
        {
            // remove quotations "" ''  "(/)|(\\,)
            parts[i] = parts[i].replace("\"","").toLowerCase();     
            parts[i] = parts[i].replace("'","");
            
            System.out.println("part is "+i+ ":"+parts[i]);
        }
        
        switch(parts[2]){
            case "en":
                parts[2]="english";
                break;
            case "eng":
                parts[2]="english";
                break;
            case "fr":
                parts[2]="french";
                break;
            case "es":
                parts[2]="spanish";
                break;
            case "it":
                parts[2]="italian";
                break;
            default:
                break;
        }
        
        switch(parts[4]){
            case "en":
                parts[4]="english";
                break;
            case "eng":
                parts[4]="english";
                break;
            case "fr":
                parts[4]="french";
                break;
            case "es":
                parts[4]="spanish";
                break;
            case "it":
                parts[4]="italian";
                break;
            default:
                break;
        }
        
        //if category field not empty have to reuse it
        if(parts[CATEGORY].length()>0){
            categories = parts[CATEGORY].split("(/)|(\\,)|(\\;)|(\\\\)",-1);
        }
        
        System.out.println("Category seperator size  " +categories.length);
        
        // dont want to share contexts so clear
        src_contexts = null;
        targ_contexts = null;
        
        if (parts[SRC_CONTEXT].length()>0){
            src_contexts = parts[SRC_CONTEXT].split("(/)|(\\,)|(\\;)|(\\\\)" ,-1);
        }
         
        if (parts[TARG_CONTEXT].length()>0){
            targ_contexts = parts[TARG_CONTEXT].split("(/)|(\\,)|(\\;)|(\\\\)" ,-1);
        }
        
        System.out.println("\n**********parsed line: "+k);
        k++;
        
        System.out.println("\n*categories array: "+Arrays.toString(categories));
        System.out.println("\n*src_contexts array: "+Arrays.toString(src_contexts));
        System.out.println("\n*targ_contexts array: "+Arrays.toString(targ_contexts));
        return true;
        
    }
    
    
    public static boolean checkEnglishTerm(long term_lang_id) throws SQLException{
        
        Statement st = null; 
        long eng_lang=0;
        
        try{

            st =con.createStatement();
            rs = st.executeQuery("Select id from languages where language like 'english';");
               
               
            while(rs.next()){
                eng_lang=rs.getLong("id");
            }

           }
           catch(SQLException e){
               e.printStackTrace();
           }
        
        if(eng_lang==term_lang_id){
            System.out.println("ENGLISH term_id: "+term_lang_id);
            return true;
        }
        else{
            System.out.println("NOT ENGLISH term_id: "+term_lang_id);
            return false;
        }
        
    }
    
    // HTTP GET request
    public String sendGet(String term) throws Exception {

            String url = "http://words.bighugelabs.com/api/2/d5813e4fd3350fc0199ce22926247826/"+term+"/json";

            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            String USER_AGENT = "Mozilla/5.0";
            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);
            
            if(responseCode==404){
               return null;
            }
            else{

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
            }
            in.close();

            
            return(response.toString());
            }

    }
    
    public void insertResults(Object obj, long term_id) throws SQLException{
        
            
        JSONObject jsonObject = (JSONObject) obj;
        
        List<String> nouns = new ArrayList<>();
        List<String> verbs = new ArrayList<>();
        
        if(jsonObject.containsKey("noun")){
            JSONObject subitem_noun = (JSONObject) jsonObject.get("noun");
            JSONArray sub_subitem_noun_syn =   (JSONArray) subitem_noun.get("syn");
                Iterator<String> iterator_noun = sub_subitem_noun_syn.iterator();
                while (iterator_noun.hasNext()) {
                    nouns.add(iterator_noun.next());
                }
        }
        else {
            System.out.println("no nouns");
        }
        
        if(jsonObject.containsKey("verb")){
            JSONObject subitem_verb = (JSONObject) jsonObject.get("verb");
            JSONArray sub_subitem_verb_syn =   (JSONArray) subitem_verb.get("syn");
                    Iterator<String> iterator_verb = sub_subitem_verb_syn.iterator();
                    while (iterator_verb.hasNext()) {
                        verbs.add(iterator_verb.next());
                    }
        }
        else {
            System.out.println("no verbs");
        }
        
        int count_nouns=0;
        if(nouns.size()>3){
            count_nouns = 3;
        }
        else{
            count_nouns = nouns.size();
        }
            
        int count_verbs=0;
        if(verbs.size()>3){
            //System.out.println("More than 5 verbs returned");
            count_verbs = 3;
        }
        else
        {
            count_verbs = verbs.size();
        }
     
        System.out.println("Result set id is: "+term_id);
        
        for (int i = 0; i < count_nouns; i++) {
            System.out.println("nouns: "+(nouns.get(i)));
            long syn_id = _synonymhtc.getID(nouns.get(i));
            long term_has_syn_id = Lookuper.Synonymlookup(con, term_id, syn_id);      
        }
        

        for (int i = 0; i < count_verbs; i++) {
            System.out.println("verbs: "+(verbs.get(i)));
            long syn_id = _synonymhtc.getID(verbs.get(i));
            long term_has_syn_id = Lookuper.Synonymlookup(con, term_id, syn_id);
            
        }
     
    }
    
}