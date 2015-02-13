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
class TermLookuper {
    
    static public long lookup(Connection con,String term, long lang_id) throws SQLException{

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
}
