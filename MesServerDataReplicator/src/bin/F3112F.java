package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class F3112F {

	public F3112F() {
		
	}

	public void executeImport(Connection connA,Connection connB) {
		
		PreparedStatement stmA = null;
	    PreparedStatement stmB = null;
	    ResultSet rs = null;
	    double rows = 0;
	    double data = 0;
	    String previouspercentage = "";
	    DecimalFormat df = new DecimalFormat("#");
	    
	    LogWriter.write("Importazione dati su tabella di frontiera: F3112F - [ Cicli Ordini di Produzione ]");
	    
		System.out.println("");
	    System.out.println("Importazione dati su tabella di frontiera: F3112F - [ Cicli Ordini di Produzione ]");
        
        try {
	        stmB = connB.prepareStatement("DELETE FROM MES.F3112F");
	        stmB.execute();
	        
	        LogWriter.write("Pulitura tabella: .................................................................................................... [ OK ]");	        						
			System.out.println("Pulitura tabella: .................................................................................................... [ OK ]");
	        
	        stmA = connA.prepareStatement("SELECT COUNT(*) FROM F3112 WHERE EXISTS(SELECT 'X' FROM F4801 WHERE WACO IN ('00020','00021') AND WADCTO != 'ET' AND WASRST < '94' AND WADOCO = WLDOCO)");
	        rs = stmA.executeQuery();
	        
	        rows = 0;
	        
	        while (rs.next()) {
	        	rows = rs.getInt(1);
	        }
	        
	        stmA = connA.prepareStatement("SELECT * FROM F3112 WHERE EXISTS(SELECT 'X' FROM F4801 WHERE WACO IN ('00020','00021') AND WADCTO != 'ET' AND WASRST < '94' AND WADOCO = WLDOCO)");
	        stmB = connB.prepareStatement("INSERT INTO F3112F VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	
	        rs = stmA.executeQuery();
	        
	        data = 0;
	        previouspercentage = "";
	        String dot = "";
            
	        while (rs.next()) {
	            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
	                stmB.setObject(i + 1, rs.getObject(i + 1));
	            }
	            stmB.executeUpdate();
	            
	            data = data + 1;
	            String percentage = df.format((data/rows*100));
    		
	            if(!percentage.equals(previouspercentage)){
	            	if((data == rows)&&(rows < 100)){
	            		for(int i=0;i<=(100-rows);i++){
	            			dot = dot + ".";
	            			System.out.print("Inserimento dati: " + dot + " [ " + percentage + " % ]\r");
	            		}
	            	}else{
		            	System.out.print("Inserimento dati: " + dot + " [ " + percentage + " % ]\r");
		            	dot = dot + ".";
	            	}
	            }
	            previouspercentage = percentage;
	        }
        
        } catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
        
        LogWriter.write("Inserimento dati: .................................................................................................... [ OK ]");
        LogWriter.write("Record inseriti: [ " + (int) data + " ]");  
        
        System.out.println("");
        System.out.println("Record inseriti: [ " + (int) data + " ]");        
	    
	}
}
