package dataimporter;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;


/**
 *
 * @author Katie
 */
public class LookupTable {
    
    protected Hashtable<String, Long> hashtable;
    
    
    protected String select_id_sql="";
    protected String insert_id_sql="";
    protected String load_sql="";
    protected Connection db_con=null; 
    protected Statement stmt=null;
    protected ResultSet rs=null;
    protected PreparedStatement pst = null; 
    
    //cache in memory fast lookuos
        
     // create an instance of con not multiple
    //abstartact class will use any connector
    // load query should have a field 1 for key(string) and 2 for id (int)
    // insert sql should have 1 parameter for key(string) e.g "Insert into languages (language) VALUES (?);" 
    // select query should return 1 row with 1 field for id int e.g Select id from languages where language=? LIMIT 1
    LookupTable(Connection con, String insert_id,String select_id, String load) throws SQLException{
        
        this.insert_id_sql= insert_id;
        this.select_id_sql=select_id;
        this.load_sql=load;    
        this.db_con=con;
        this.hashtable=  new Hashtable<String, Long>();
        if(db_con !=null && db_con.isValid(1) && !db_con.isReadOnly() && insert_id_sql.length()>0 && select_id_sql.length()>0){
            //fetching data from db directly to hashtable
            this.reload();
            return;
        }
       throw new SQLException("unknown error");
        
    }
    
    
    // when doesn.t have to load 
    // don't have to use it
    // read abut throws in function declaration
    //LookupTable(Connection con,String insert_id,String select_id) throws SQLException{
        
      // null as load optional  
      //LookupTable(con,insert_id,select_id,"");
        
    //}
    
    // closing used objects
    protected void clearDBObjects() {
        if(this.rs!=null){
            try{
                this.rs.close();
            }catch (Exception e){}
            this.rs=null;
        }
        if(this.stmt!=null){
            try{
                this.stmt.close();
            }catch (Exception e){} 
            this.stmt=null;
        }
        if(this.pst!=null){
            try{
                this.pst.close();
            }catch (Exception e){}
            this.pst=null;
        }
    }
    
    protected void loadFromDB() throws SQLException{
        
        //execute only when the query (loaddb) is not empty
        if(this.load_sql!=null && ""!=this.load_sql && this.load_sql.length()>0){
            this.stmt=this.db_con.createStatement();
            //loading exiting values to hashtable if something is already there
            this.rs=stmt.executeQuery(this.load_sql);
            while (rs.next()){
                this.hashtable.put(rs.getString(1), (long)rs.getInt(2));
            }
            this.clearDBObjects();
        }
    }
    
    public long getID(String key)throws SQLException{
        return this.getID(key, null);
    }
    
    public long getID(String key, Long id)throws SQLException{
        
        // error
        if(key==null || key.length()==0)
        {
            return -1; //error
        }
        
        // get key
        Long ret=this.hashtable.get(key);
        
        // if already exists return
        if (ret != null){
            return ret;
        }
        try{
        
            // 
            System.out.println("isql:"+this.insert_id_sql+" | "+key+" | "+id);
            this.pst = this.db_con.prepareStatement(this.insert_id_sql  ,
                                  Statement.RETURN_GENERATED_KEYS);
            //
            this.pst.setString(1, key);
            if(id!=null){
                this.pst.setLong(2,id);
            }

            if( pst.executeUpdate()!=0){
                // if successful
                // return new id(primary key)
                try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        // assigning new value to new field in hashtable (cache)
                        // use only with known size, not unlimitted amount of element e.g. terms
                        hashtable.put(key, generatedKeys.getLong(1));
                        long x = generatedKeys.getInt(1);
                        generatedKeys.close();
                        this.clearDBObjects();    
                        return x;
                    }
                    System.err.println("warning: xxxxx");
                              //TODO: Report error
                }                                
            }
            
            this.clearDBObjects();
            
        }
        catch(Exception e){
            System.err.println("warning:"+e.getMessage()); 
            this.clearDBObjects();
        }
        
        //error condition value in database already there but not present in hashtable
        this.pst = this.db_con.prepareStatement(this.select_id_sql  ,
                                 Statement.RETURN_GENERATED_KEYS);
        this.pst.setString(1, key);


        this.rs = pst.executeQuery();
        long x=0;
        while (this.rs.next()) {
            this.hashtable.put(key, (long)this.rs.getInt(1));
            x=(long)this.rs.getInt(1);
            break;
        }
        this.clearDBObjects();
        return x;
      
    } // close getID()
    
    public void clear(){
        this.hashtable.clear();
    }
    
    public int size(){
        return this.hashtable.size();
    }
    
    // wipe data
    // if error hash will be consistant in db and memory
    public void reload() throws SQLException{
        this.clear();
        this.loadFromDB();
    }
    
    public void dump(){
         for (String key : hashtable.keySet()) {
                if (key!=null)
                    System.out.println("key: " + key + " value: " + hashtable.get(key));
                }
    }
}



/*
LookupTable languages = new LookupTable(
    db_con,
    "Insert into languages (language) VALUES (?);",//insert
    "Select id from languages where language=?;",//select
    "select language,id from languages;"//load
);

LookupTable categories = new LookupTable(
    db_con,
    "Insert into categories (language) VALUES (?);",//insert
    "Select id from categories where categories=?;",//select
    "select categories,id from categories;"//load
);

long lang_id = languages.get("english");//returns id

long cat_id = categories.get("war");//returns id
*/