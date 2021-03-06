package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class F0006F {

	public F0006F() {

	}

	public void executeImport(Connection connA,Connection connB) {
		
		PreparedStatement stmA = null;
        PreparedStatement stmB = null;
        ResultSet rs = null;
        double rows = 0;
        double data = 0;
        String previouspercentage = "";
        DecimalFormat df = new DecimalFormat("#");
        
        LogWriter.write("Importazione dati su tabella di frontiera: F0006F - [ Anagrafica Centri ]");
        
		System.out.println("");
		System.out.println("Importazione dati su tabella di frontiera: F0006F - [ Anagrafica Centri ]");
        
        try {
			stmB = connB.prepareStatement("DELETE FROM MES.F0006F");
	        stmB.execute();
	        
	        LogWriter.write("Pulitura tabella: .................................................................................................... [ OK ]");					
	        System.out.println("Pulitura tabella: .................................................................................................... [ OK ]");
	        
	        stmA = connA.prepareStatement("SELECT COUNT(*) FROM F0006 WHERE MCSTYL IN ('BP','WC','CP')");
	        rs = stmA.executeQuery();
	        
	        rows = 0;
	        
	        while (rs.next()) {
				rows = rs.getInt(1);
			}
	        
	        stmA = connA.prepareStatement("SELECT * FROM F0006 WHERE MCSTYL IN ('BP','WC','CP')");
			stmB = connB.prepareStatement("INSERT INTO F0006F VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
