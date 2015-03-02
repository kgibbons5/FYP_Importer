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
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException, IOException {
        
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
        
        
        long src_lang_id=0;
        long targ_lang_id=0;
        long src_term_id=0;
        long targ_term_id=0;
        long translation_id=0;
        long sim_translation_id=0;
        
       
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
                
                
                // put into each of their own try catch
                try{
                    for(int j=0; j<categories.length; j++)
                    {
                       System.out.println("Inside categories "+categories[j]);
                       long category_id=_categorieshtc.getID(categories[j]);
                       System.out.println("\nCategory id is:"+ category_id);
                       long src_term_has_cat_id = Lookuper.Categorylookup(con, src_term_id,category_id );
                       long targ_term_has_cat_id = Lookuper.Categorylookup(con, targ_term_id, category_id);
                       System.out.println("\nsrc_term_has_context_id : "+src_term_has_cat_id );
                       System.out.println("\ntarg_term_has_context_id : "+targ_term_has_cat_id );
                    }
                }
                 catch(Exception e){
                     System.err.println("Categories warning:"+e.getMessage()); 
                 }
                
                
                
                try{
                    for(int j=0; j<src_contexts.length; j++)
                    {
                       System.out.println("Inside categories "+src_contexts[j]);
                       long context_id=_contexthtc.getID(src_contexts[j],src_lang_id);
                       System.out.println("\nCategory id is:"+ context_id);
                       long src_term_has_contetx_id = Lookuper.Contextlookup(con, src_term_id,context_id );
                       System.out.println("\nsrc_term_has_context_id : "+src_term_has_contetx_id );
                    }
                }
                catch(Exception e){
                     System.err.println("Source context warning:"+e.getMessage()); 
                 }
                 
                 
                try{
                    for(int j=0; j<targ_contexts.length; j++)
                    {
                       System.out.println("Inside categories "+targ_contexts[j]);
                       long context_id=_contexthtc.getID(targ_contexts[j],targ_lang_id);
                       System.out.println("\nCategory id is:"+ context_id);
                       long targ_term_has_context_id = Lookuper.Contextlookup(con, targ_term_id, context_id);
                       System.out.println("\ntarg_term_has_context_id : "+targ_term_has_context_id );
                    }
                }
                catch(Exception e){
                    System.err.println("Target context warning:"+e.getMessage()); 
                }
                
                
                
                // insert into translations table
               translation_id = Lookuper.Translationslookup(con, src_term_id, targ_term_id);
                
                
                
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
            //since context is language driven we don't know which language to laod
            _contexthtc =new LookupTable(con, 
                "Insert into context (context,language) VALUES (?,?);", 
                "Select id from context where context=? and language=? limit 1;",
                null
            ); 
                    
            return true;
        }

        return false;
    }
       
    /*
      
    static final int CATEGORY = 0;
    static final int SRC_TERM = 1;
    static final int SRC_LANG = 2;
    static final int SRC_CONTEXT = 3;
    static final int TARG_LANG = 4;
    static final int TARG_TERM = 5;
    static final int TARG_CONTEXT = 6;
    
    */

    public static boolean parsed(String line){
      
       parts = null;
       // keep commas within column
       parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",7);
       
        for(int i=0; i<parts.length; i++)
        {
            // remove quotations
            parts[i] = parts[i].replace("\"","").toLowerCase();
            System.out.println("part is "+i+ ":"+parts[i]);
        }
         
        if(parts[CATEGORY].length()>0){
             //if category field not empty have to reuse it
            //categories = parts[CATEGORY].split("/",-1);
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
}