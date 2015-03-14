package dataimporter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Katie
 */
public class Lookuper {
    
    static public long Termlookup(Connection con, String term, long lang_id) throws SQLException{

        long x=0;
        PreparedStatement pst=null;
        
        try{
            
            pst = con.prepareStatement("Insert into terms (language_id, term) VALUES (?,?);"  ,
                                                  Statement.RETURN_GENERATED_KEYS);
            pst.setLong(1, lang_id);
            pst.setString(2, term);
            if( pst.executeUpdate()!=0){
                // if successful
                // return new id(primary key)
                try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                    if (generatedKeys.next()) {                        
                        x = generatedKeys.getInt(1);
                        generatedKeys.close();
                        pst.close();
                        return x;
                    }
                }                                
            }   
            
            pst.close();
        }catch(Exception e){
            pst.close();
        }

        pst = con.prepareStatement("Select id from terms where language_id=? and term=? limit 1;" ,
                                      Statement.RETURN_GENERATED_KEYS);
        pst.setLong(1, lang_id);
        pst.setString(2, term);
        ResultSet rs=pst.executeQuery();
        x=0;
        while (rs.next()) {
            x=(long)rs.getInt(1);
            break;
        }
        rs.close();
        pst.close();
        return x;     
    }
   
   
    static public long Categorylookup(Connection con, long terms_id, long categories_id) throws SQLException{

        long x=0;
        PreparedStatement pst=null;
        
        try{
            
            pst = con.prepareStatement("Insert into terms_has_categories (terms_id, categories_id) VALUES (?,?);"  ,
                                                  Statement.RETURN_GENERATED_KEYS);
            pst.setLong(1, terms_id);
            pst.setLong(2, categories_id);
            if( pst.executeUpdate()!=0){
                // if successful
                // return new id(primary key)
                try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                    if (generatedKeys.next()) {                        
                        x = generatedKeys.getInt(1);
                        generatedKeys.close();
                        pst.close();
                        return x;
                    }
                }                                
            }   
            
            pst.close();
        }catch(Exception e){
            pst.close();
        }

        pst = con.prepareStatement("Select id from terms_has_categories where terms_id=? and categories_id=? limit 1;" ,
                                      Statement.RETURN_GENERATED_KEYS);
        pst.setLong(1, terms_id);
        pst.setLong(2, categories_id);
        ResultSet rs=pst.executeQuery();
        x=0;
        while (rs.next()) {
            x=(long)rs.getInt(1);
            break;
        }
        rs.close();
        pst.close();
        return x;     
    }
    
    static public long Contextlookup(Connection con, long terms_id, long context_id) throws SQLException{

       long x=0;
       PreparedStatement pst=null;

       try{

           pst = con.prepareStatement("Insert into terms_has_context (terms_id, context_id) VALUES (?,?);"  ,
                                                 Statement.RETURN_GENERATED_KEYS);
           pst.setLong(1, terms_id);
           pst.setLong(2, context_id);
           if( pst.executeUpdate()!=0){
               // if successful
               // return new id(primary key)
               try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                   if (generatedKeys.next()) {                        
                       x = generatedKeys.getInt(1);
                       generatedKeys.close();
                       pst.close();
                       return x;
                   }
               }                                
           }   

           pst.close();
       }catch(Exception e){
           pst.close();
       }

       pst = con.prepareStatement("Select id from terms_has_context where terms_id=? and context_id=? limit 1;" ,
                                     Statement.RETURN_GENERATED_KEYS);
       pst.setLong(1, terms_id);
       pst.setLong(2, context_id);
       ResultSet rs=pst.executeQuery();
       x=0;
       while (rs.next()) {
           x=(long)rs.getInt(1);
           break;
       }
       rs.close();
       pst.close();
       return x;     
   }
    
    
    static public long Synonymlookup(Connection con, long terms_id, long synonyms_id) throws SQLException{

       long x=0;
       PreparedStatement pst=null;

       try{

           pst = con.prepareStatement("Insert into terms_has_synonyms (terms_id, synonyms_id) VALUES (?,?);"  ,
                                                 Statement.RETURN_GENERATED_KEYS);
           pst.setLong(1, terms_id);
           pst.setLong(2, synonyms_id);
           if( pst.executeUpdate()!=0){
               // if successful
               // return new id(primary key)
               try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                   if (generatedKeys.next()) {                        
                       x = generatedKeys.getInt(1);
                       generatedKeys.close();
                       pst.close();
                       return x;
                   }
               }                                
           }   

           pst.close();
       }catch(Exception e){
           pst.close();
       }

       pst = con.prepareStatement("Select id from terms_has_synonyms where terms_id=? and synonyms_id=? limit 1;" ,
                                     Statement.RETURN_GENERATED_KEYS);
       pst.setLong(1, terms_id);
       pst.setLong(2, synonyms_id);
       ResultSet rs=pst.executeQuery();
       x=0;
       while (rs.next()) {
           x=(long)rs.getInt(1);
           break;
       }
       rs.close();
       pst.close();
       return x;     
    }
    
    
    static public long Translationslookup(Connection con, long src_term_id, long targ_term_id) throws SQLException{

       long x=0;
       PreparedStatement pst=null;

       try{

           pst = con.prepareStatement("Insert into translations (src_term_id, targ_term_id) VALUES (?,?);"  ,
                                                 Statement.RETURN_GENERATED_KEYS);
           pst.setLong(1, src_term_id);
           pst.setLong(2, targ_term_id);
           if( pst.executeUpdate()!=0){
               // if successful
               // return new id(primary key)
               try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                   if (generatedKeys.next()) {                        
                       x = generatedKeys.getInt(1);
                       generatedKeys.close();
                       pst.close();
                       return x;
                   }
               }                                
           }   

           pst.close();
       }catch(Exception e){
           pst.close();
       }

       pst = con.prepareStatement("Select id from translations where src_term_id=? and targ_term_id=? limit 1;" ,
                                     Statement.RETURN_GENERATED_KEYS);
       pst.setLong(1, src_term_id);
       pst.setLong(2, targ_term_id);
       ResultSet rs=pst.executeQuery();
       x=0;
       while (rs.next()) {
           x=(long)rs.getInt(1);
           break;
       }
       rs.close();
       pst.close();
       return x;     
   }
    
    
    static public long SimTranslationslookup(Connection con, long src_term_id, long targ_term_id, float similarity_score) throws SQLException{

       long x=0;
       PreparedStatement pst=null;

       try{

           pst = con.prepareStatement("Insert into translations (src_term_id, targ_term_id, similarity_score) VALUES (?,?,?);"  ,
                                                 Statement.RETURN_GENERATED_KEYS);
           pst.setLong(1, src_term_id);
           pst.setLong(2, targ_term_id);
           pst.setFloat(3, similarity_score);
           
           if( pst.executeUpdate()!=0){
               // if successful
               // return new id(primary key)
               try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                   if (generatedKeys.next()) {                        
                       x = generatedKeys.getInt(1);
                       generatedKeys.close();
                       pst.close();
                       return x;
                   }
               }                                
           }   

           pst.close();
       }catch(Exception e){
           pst.close();
       }

       pst = con.prepareStatement("Select id from translations where src_term_id=? and targ_term_id=? and similarity_score=? limit 1;" ,
                                     Statement.RETURN_GENERATED_KEYS);
       pst.setLong(1, src_term_id);
       pst.setLong(2, targ_term_id);
        pst.setFloat(3, similarity_score);
       ResultSet rs=pst.executeQuery();
       x=0;
       while (rs.next()) {
           x=(long)rs.getInt(1);
           break;
       }
       rs.close();
       pst.close();
       return x;     
   }
    
}
