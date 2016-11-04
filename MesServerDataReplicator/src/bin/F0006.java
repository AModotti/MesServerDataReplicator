package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class F0006 {

	public F0006() {
		
	}

	public void executeUpdate(Connection connB) {
		
		PreparedStatement stmB = null;
		        
		LogWriter.write("Aggiornamento dati modificati tabella: F0006 - [ Anagrafica Centri ]");
		
		System.out.println("");
        System.out.println("Aggiornamento dati modificati tabella: F0006 - [ Anagrafica Centri ]");
        
        try {
	        stmB = connB.prepareStatement("UPDATE F0006 A INNER JOIN F0006F B " 
	                                    + "    ON (A.MCMCU = B.MCMCU) " 
	                                    + "   SET A.MCSTYL = B.MCSTYL, A.MCDC   = B.MCDC,   A.MCLDM  = B.MCLDM,  A.MCCO   = B.MCCO,   A.MCAN8  = B.MCAN8,  A.MCAN8O = B.MCAN8O, A.MCCNTY = B.MCCNTY," 
	                                    + "       A.MCADDS = B.MCADDS, A.MCFMOD = B.MCFMOD, A.MCDL01 = B.MCDL01, A.MCDL02 = B.MCDL02, A.MCDL03 = B.MCDL03, A.MCDL04 = B.MCDL04," 
	                                    + "       A.MCRP01 = B.MCRP01, A.MCRP02 = B.MCRP02, A.MCRP03 = B.MCRP03, A.MCRP04 = B.MCRP04, A.MCRP05 = B.MCRP05, A.MCRP06 = B.MCRP06," 
	                                    + "       A.MCRP07 = B.MCRP07, A.MCRP08 = B.MCRP08, A.MCRP09 = B.MCRP09, A.MCRP10 = B.MCRP10, A.MCRP11 = B.MCRP11, A.MCRP12 = B.MCRP12,"
	                                    + "       A.MCRP13 = B.MCRP13, A.MCRP14 = B.MCRP14, A.MCRP15 = B.MCRP15, A.MCRP16 = B.MCRP16, A.MCRP17 = B.MCRP17, A.MCRP18 = B.MCRP18,"
	                                    + "       A.MCRP19 = B.MCRP19, A.MCRP20 = B.MCRP20, A.MCRP21 = B.MCRP21, A.MCRP22 = B.MCRP22, A.MCRP23 = B.MCRP23, A.MCRP24 = B.MCRP24,"
	                                    + "       A.MCRP25 = B.MCRP25, A.MCRP26 = B.MCRP26, A.MCRP27 = B.MCRP27, A.MCRP28 = B.MCRP28, A.MCRP29 = B.MCRP29, A.MCRP30 = B.MCRP30,"
	                                    + "       A.MCTA   = B.MCTA,   A.MCTXJS = B.MCTXJS, A.MCTXA1 = B.MCTXA1, A.MCEXR1 = B.MCEXR1, A.MCTC01 = B.MCTC01, A.MCTC02 = B.MCTC02,"
	                                    + "       A.MCTC03 = B.MCTC03, A.MCTC04 = B.MCTC04, A.MCTC05 = B.MCTC05, A.MCTC06 = B.MCTC06, A.MCTC07 = B.MCTC07, A.MCTC08 = B.MCTC08,"
	                                    + "       A.MCTC09 = B.MCTC09, A.MCTC10 = B.MCTC10, A.MCND01 = B.MCND01, A.MCND02 = B.MCND02, A.MCND03 = B.MCND03, A.MCND04 = B.MCND04,"
	                                    + "       A.MCND05 = B.MCND05, A.MCND06 = B.MCND06, A.MCND07 = B.MCND07, A.MCND08 = B.MCND08, A.MCND09 = B.MCND09, A.MCND10 = B.MCND10,"
	                                    + "       A.MCCC01 = B.MCCC01, A.MCCC02 = B.MCCC02, A.MCCC03 = B.MCCC03, A.MCCC04 = B.MCCC04, A.MCCC05 = B.MCCC05, A.MCCC06 = B.MCCC06,"
	                                    + "       A.MCCC07 = B.MCCC07, A.MCCC08 = B.MCCC08, A.MCCC09 = B.MCCC09, A.MCCC10 = B.MCCC10, A.MCPECC = B.MCPECC, A.MCALS =  B.MCALS,"
	                                    + "       A.MCISS  = B.MCISS,  A.MCGLBA = B.MCGLBA, A.MCALCL = B.MCALCL, A.MCLMTH = B.MCLMTH, A.MCLF   = B.MCLF,   A.MCOBJ1 = B.MCOBJ1,"
	                                    + "       A.MCOBJ2 = B.MCOBJ2, A.MCOBJ3 = B.MCOBJ3, A.MCSUB1 = B.MCSUB1, A.MCTOU  = B.MCTOU,  A.MCSBLI = B.MCSBLI, A.MCANPA = B.MCANPA,"
	                                    + "       A.MCCT   = B.MCCT,   A.MCCERT = B.MCCERT, A.MCMCUS = B.MCMCUS, A.MCBTYP = B.MCBTYP, A.MCPC   = B.MCPC,   A.MCPCA  = B.MCPCA,  A.MCPCC  = B.MCPCC,"
	                                    + "       A.MCINTA = B.MCINTA, A.MCINTL = B.MCINTL, A.MCD1J  = B.MCD1J,  A.MCD2J  = B.MCD2J,  A.MCD3J  = B.MCD3J,  A.MCD4J  = B.MCD4J,  A.MCD5J  = B.MCD5J,"
	                                    + "       A.MCD6J  = B.MCD6J,  A.MCFPDJ = B.MCFPDJ, A.MCCAC  = B.MCCAC,  A.MCPAC  = B.MCPAC,  A.MCEEO  = B.MCEEO,  A.MCERC  = B.MCERC,  A.MCUSER = B.MCUSER,"
	                                    + "       A.MCPID  = B.MCPID,  A.MCUPMJ = B.MCUPMJ, A.MCJOBN = B.MCJOBN, A.MCUPMT = B.MCUPMT, A.MCBPTP = B.MCBPTP, A.MCAPSB = B.MCAPSB, A.MCTSBU = B.MCTSBU "
	                                    + " WHERE CONCAT(A.MCUPMJ,A.MCUPMT) < CONCAT(B.MCUPMJ,B.MCUPMT)");

	        int rowupdated = stmB.executeUpdate();
	        
	        LogWriter.write("Aggiornamento dati: .................................................................................................. [ OK ]");
			LogWriter.write("Record aggiornati: [ " + rowupdated + " ]");
			
	        System.out.println("Aggiornamento dati: .................................................................................................. [ OK ]");
	        System.out.println("Record aggiornati: [ " + rowupdated + " ]");
        
        } catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
        
	}
	
	public void executeInsert(Connection connB) {
		
		PreparedStatement stmB = null;
        
        try {
	        stmB = connB.prepareStatement("INSERT INTO F0006 SELECT * FROM F0006F A WHERE NOT EXISTS(SELECT 'X' FROM F0006 B WHERE A.MCMCU = B.MCMCU)");
	        
	        int newrow = stmB.executeUpdate();
	        
	        LogWriter.write("Inserimento nuovi dati: .............................................................................................. [ OK ]");
	        LogWriter.write("Record inseriti: [ " + newrow + " ]");
	        				
	        System.out.println("Inserimento nuovi dati: .............................................................................................. [ OK ]");
	        System.out.println("Record inseriti: [ " + newrow + " ]");
        
        } catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
        
	}

	public void executeDelete(Connection connB) {
	
		PreparedStatement stmB = null;
        
        try {

	        stmB = connB.prepareStatement("DELETE FROM F0006 WHERE NOT EXISTS(SELECT 'X' FROM F0006F B WHERE MCMCU = B.MCMCU)");
	        
	        int rowdeleted = stmB.executeUpdate();
	        
	        LogWriter.write("Cancellazione dati non necessari: .................................................................................... [ OK ]");
	        LogWriter.write("Record cancellati: [ " + rowdeleted + " ]");				
	        
	        System.out.println("Cancellazione dati non necessari: .................................................................................... [ OK ]");
	        System.out.println("Record cancellati: [ " + rowdeleted + " ]");
	        
        } catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}    
		
	}
}
