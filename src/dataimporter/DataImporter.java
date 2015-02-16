package dataimporter;
import java.sql.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import static java.security.AccessController.getContext;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.swing.JOptionPane;
/*
 *
 * @author Katie
 */
public class DataImporter {
    
    static String filename = "C://Users//Katie//Documents//College//FYP//data//glossary_italian_english_conflict_terrorism_1.csv";
    static Connection con = null; 
    static BufferedReader br = null;
    static PreparedStatement pst = null; 
    
    
    static LookupTable _languagehtc = null;
    static LookupTable _categorieshtc = null;
    static LookupTable _contexthtc = null;
    
    public static String[]parts = null;
    public static String[] categories = null;

    public static boolean categories_set = false;
    
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
        //long category_id=0;
        
       
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
                
                 for(int j=0; j<src_contexts.length; j++)
                {
                   System.out.println("Inside categories "+src_contexts[j]);
                   long context_id=_contexthtc.getID(src_contexts[j],src_lang_id);
                   System.out.println("\nCategory id is:"+ context_id);
                   long src_term_has_contetx_id = Lookuper.Contextlookup(con, src_term_id,context_id );
                   System.out.println("\nsrc_term_has_context_id : "+src_term_has_contetx_id );
                }
                 
                 
                  for(int j=0; j<targ_contexts.length; j++)
                {
                   System.out.println("Inside categories "+targ_contexts[j]);
                   long context_id=_contexthtc.getID(targ_contexts[j],targ_lang_id);
                   System.out.println("\nCategory id is:"+ context_id);
                   long targ_term_has_context_id = Lookuper.Contextlookup(con, targ_term_id, context_id);
                   System.out.println("\ntarg_term_has_context_id : "+targ_term_has_context_id );
                }
                
                
                // insert into translations table
                pst = con.prepareStatement("Insert into translations (src_term_id, targ_term_id) VALUES (?,?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                pst.setLong(1, src_term_id);
                pst.setLong(2, targ_term_id);

                pst.executeUpdate();

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
      
       parts =null;
       parts=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
       
        for(int i=0; i<parts.length; i++)
        {
            // remove quotations
            parts[i] = parts[i].replace("\"","").toLowerCase();
        }
         
        
        if (parts[CATEGORY].length()>0){
             //if category field not empty have to reuse it
            categories = parts[CATEGORY].split("/",-1);
        }
        // always greater than 0???????????????????
        System.out.println("Category seperator size  " +categories.length);
        
       
        // dont want to share contexts
        src_contexts=null;
        targ_contexts=null;
        
         if (parts[SRC_CONTEXT].length()>0){
            src_contexts = parts[SRC_CONTEXT].split("/",-1);
        }
         
          if (parts[TARG_CONTEXT].length()>0){
            targ_contexts = parts[TARG_CONTEXT].split("/",-1);
        }
        
       
        
        System.out.println("\n**********parsed line: "+k);
        k++;
        
        return true;
        
    }
    
}
