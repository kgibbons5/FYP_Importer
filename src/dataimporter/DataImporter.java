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
    public static Map<String, Long> categories = null;
    public static boolean categories_set = false;
    
    public static Map<String, Long>  src_contexts = null;
    public static Map<String, Long>  targ_contexts = null;
    
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
        long category_id=0;
        long src_context_id=0;
        long targ_context_id=0;
        
       
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
                src_term_id = TermLookuper.lookup(con, parts[SRC_TERM], src_lang_id);
                targ_term_id = TermLookuper.lookup(con, parts[TARG_TERM], targ_lang_id);
                
                
                // insert into translations table
                pst = con.prepareStatement("Insert into translations (src_term_id, targ_term_id) VALUES (?,?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                pst.setLong(1, src_term_id);
                pst.setLong(2, targ_term_id);

                pst.executeUpdate();

                
                // if set to false
                if(!categories_set){
                    
                    for(Map.Entry<String, Long> entry : categories.entrySet()) {
                        // storing id and cat name in categories map
                        entry.setValue(_categorieshtc.getID(entry.getKey()));
                        System.out.println("Entry in categories is: "+entry);
                        category_id= entry.getValue();
                    }
                    
                    categories_set = true;
               }
               
           
                System.out.println("CATEGORY ID IS: " +category_id);
                
               
                
                pst = con.prepareStatement("Insert into terms_has_categories (terms_id, categories_id) VALUES (?,?);"  ,
                                    Statement.RETURN_GENERATED_KEYS);
                pst.setLong(1, src_term_id);
                pst.setLong(2, category_id);

                pst.executeUpdate();

                pst = con.prepareStatement("Insert into terms_has_categories (terms_id, categories_id) VALUES (?,?);"  ,
                                    Statement.RETURN_GENERATED_KEYS);
                pst.setLong(1, targ_term_id);
                pst.setLong(2, category_id);

                pst.executeUpdate(); 
            

                
                for(Map.Entry<String, Long> entry : src_contexts.entrySet()) {
                       
                       entry.setValue(_contexthtc.getID(entry.getKey(),src_lang_id));
                       System.out.println("!!!!!!!!!!!!!Entry in src context is: "+entry);
                       
                   }
                
//                src_context_id =_contexthtc.getID(parts[SRC_CONTEXT]);
//                targ_context_id =_contexthtc.getID(parts[TARG_CONTEXT]);
//                
//                System.out.println("SOURCE CONTEXT IS "+src_context_id);
//                System.out.println("TARG CONTEXT IS "+targ_context_id);
          
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
            
            
            _contexthtc =new LookupTable(con, 
                "Insert into context (context,language) VALUES (?,?);", 
                "Select id from context where context=? limit 1;",
                "Select id,context,language from context;"
            ); 
            
            
            categories = new TreeMap<>();
            src_contexts = new TreeMap<>();
            targ_contexts = new TreeMap<>();
            
           
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
         
        String[] cat_seperator = parts[CATEGORY].split("/",-1);
        
        // always greater than 0???????????????????
        System.out.println("Category seperator size  " +cat_seperator.length);
        
        //if category field not empty have to reuse it
        //if empty flag still set to true so will have previous category?????
        //if not empty and new category add to map
        //if(cat_seperator.length>0){
        for(int i=0;i<cat_seperator.length;i++){
                
            if(cat_seperator.length>0 && !cat_seperator[i].isEmpty()){
                categories.clear();
                categories_set = false;

                for(int j=0; j<cat_seperator.length; j++)
                {
                    System.out.println("Inside category seperator "+cat_seperator[j]);
                    // put categories into categories map
                    categories.put(cat_seperator[j],null);
                }
            }        
        }
        
        src_contexts.clear();
        targ_contexts.clear();
        
        //splitting contexts
        String[] src_context_seperator = parts[SRC_CONTEXT].split("/",-1);
        String[] targ_context_seperator = parts[TARG_CONTEXT].split("/",-1);
        
        for(int i=0; i<src_context_seperator.length; i++)
        {
            src_contexts.put(src_context_seperator[i],null);
            System.out.println("Inside src context seperator "+src_context_seperator[i]);
            
        }
        
        for(int i=0; i<targ_context_seperator.length; i++)
        {
            targ_contexts.put(targ_context_seperator[i],null);
            System.out.println("Inside targ context seperator "+targ_context_seperator[i]);
            
        }
        
        System.out.println("\n**********parsed line: "+k);
        k++;
        
        return true;
        
    }
    
}
