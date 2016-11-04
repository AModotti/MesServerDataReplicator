package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class F0101 {

	public F0101() {
		
	}
	
    public void executeUpdate(Connection connB) {
		
		PreparedStatement stmB = null;
		        
		LogWriter.write("Aggiornamento dati modificati tabella: F0101 - [ Rubrica Indirizzi ]");
		
		System.out.println("");
        System.out.println("Aggiornamento dati modificati tabella: F0101 - [ Rubrica Indirizzi ]");
        
        try {
	        stmB = connB.prepareStatement("UPDATE F0101 A INNER JOIN F0101F B " 
	                                    + "    ON (A.ABAN8 = B.ABAN8) " 
	                                    + "   SET A.ABALKY	  =	B.ABALKY,    A.ABTAX 	 = B.ABTAX,     A.ABALPH	= B.ABALPH,    A.ABDC		= B.ABDC,    A.ABMCU   = B.ABMCU," 
	                                    + "       A.ABSIC 	  =	B.ABSIC,     A.ABLNGP	 = B.ABLNGP,    A.ABAT1 	= B.ABAT1,     A.ABCM		= B.ABCM,    A.ABTAXC  = B.ABTAXC,"
	                                    + "       A.ABAT2 	  =	B.ABAT2,     A.ABAT3 	 = B.ABAT3,     A.ABAT4 	= B.ABAT4,     A.ABAT5 		= B.ABAT5,   A.ABATP   = B.ABATP," 
	                                    + "       A.ABATR 	  =	B.ABATR,     A.ABATPR	 = B.ABATPR,    A.ABAB3 	= B.ABAB3,     A.ABATE 		= B.ABATE,   A.ABSBLI  = B.ABSBLI,"
	                                    + "       A.ABEFTB	  =	B.ABEFTB,    A.ABAN81	 = B.ABAN81,    A.ABAN82	= B.ABAN82,    A.ABAN83		= B.ABAN83,  A.ABAN84  = B.ABAN84,"
	                                    + "       A.ABAN86	  =	B.ABAN86,    A.ABAN85	 = B.ABAN85,    A.ABAC01	= B.ABAC01,    A.ABAC02		= B.ABAC02,  A.ABAC03  = B.ABAC03,"
	                                    + "       A.ABAC04	  =	B.ABAC04,    A.ABAC05	 = B.ABAC05,    A.ABAC06	= B.ABAC06,    A.ABAC07		= B.ABAC07,  A.ABAC08  = B.ABAC08,"
	                                    + "       A.ABAC09	  =	B.ABAC09,    A.ABAC10	 = B.ABAC10,    A.ABAC11	= B.ABAC11,    A.ABAC12		= B.ABAC12,  A.ABAC13  = B.ABAC13,"
	                                    + "       A.ABAC14	  =	B.ABAC14,    A.ABAC15	 = B.ABAC15,    A.ABAC16	= B.ABAC16,    A.ABAC17		= B.ABAC17,  A.ABAC18  = B.ABAC18,"
	                                    + "       A.ABAC19	  =	B.ABAC19,    A.ABAC20	 = B.ABAC20,    A.ABAC21	= B.ABAC21,    A.ABAC22		= B.ABAC22,  A.ABAC23  = B.ABAC23,"
	                                    + "       A.ABAC24	  =	B.ABAC24,    A.ABAC25	 = B.ABAC25,    A.ABAC26	= B.ABAC26,    A.ABAC27		= B.ABAC27,  A.ABAC28  = B.ABAC28,"
	                                    + "       A.ABAC29	  =	B.ABAC29,    A.ABAC30	 = B.ABAC30,    A.ABGLBA	= B.ABGLBA,    A.ABPTI 	    = B.ABPTI,   A.ABPDI   = B.ABPDI," 
	                                    + "       A.ABMSGA	  =	B.ABMSGA,    A.ABRMK 	 = B.ABRMK,     A.ABTXCT	= B.ABTXCT,    A.ABTX2 	    = B.ABTX2,   A.ABALP1  = B.ABALP1,"
	                                    + "       A.ABURCD	  =	B.ABURCD,    A.ABURDT	 = B.ABURDT,    A.ABURAT	= B.ABURAT,    A.ABURAB	    = B.ABURAB,  A.ABURRF  = B.ABURRF,"
	                                    + "       A.ABUSER	  =	B.ABUSER,    A.ABPID 	 = B.ABPID,     A.ABUPMJ	= B.ABUPMJ,    A.ABJOBN  	= B.ABJOBN,  A.ABUPMT  = B.ABUPMT,"
	                                    + "       A.ABPRGF	  =	B.ABPRGF,    A.ABSCCLTP	 = B.ABSCCLTP,  A.ABTICKER	= B.ABTICKER,  A.ABEXCHG 	= B.ABEXCHG, A.ABDUNS  = B.ABDUNS,"
	                                    + "       A.ABCLASS01 =	B.ABCLASS01, A.ABCLASS02 = B.ABCLASS02, A.ABCLASS03 = B.ABCLASS03, A.ABCLASS04 	= B.ABCLASS04," 
	                                    + "       A.ABCLASS05 =	B.ABCLASS05, A.ABNOE 	 = B.ABNOE,     A.ABGROWTHR = B.ABGROWTHR, A.ABYEARSTAR	= B.ABYEARSTAR,"
	                                    + "       A.ABAEMPGP  =	B.ABAEMPGP,  A.ABACTIN 	 = B.ABACTIN,   A.ABREVRNG	= B.ABREVRNG,  A.ABSYNCS 	= B.ABSYNCS, A.ABPERRS = B.ABPERRS," 
	                                    + "       A.ABCAAD	  =	B.ABCAAD "
	                                    + " WHERE CONCAT(A.ABUPMJ,A.ABUPMT) < CONCAT(B.ABUPMJ,B.ABUPMT)");

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
	        stmB = connB.prepareStatement("INSERT INTO F0101 SELECT * FROM F0101F A WHERE NOT EXISTS(SELECT 'X' FROM F0101 B WHERE A.ABAN8 = B.ABAN8)");
	        
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

	        stmB = connB.prepareStatement("DELETE FROM F0101 WHERE NOT EXISTS(SELECT 'X' FROM F0101F B WHERE ABAN8 = B.ABAN8)");
	        
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

