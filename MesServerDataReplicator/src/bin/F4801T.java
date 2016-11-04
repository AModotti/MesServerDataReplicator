package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class F4801T {

public F4801T() {
		
	}
	
    public void executeUpdate(Connection connB) {
		
		PreparedStatement stmB = null;
		        
		LogWriter.write("Aggiornamento dati modificati tabella: F4801T - [ Testate Ordini di Produzione 2 ]");

		System.out.println("");
        System.out.println("Aggiornamento dati modificati tabella: F4801T - [ Testate Ordini di Produzione 2 ]");
        
        try {
	        stmB = connB.prepareStatement("UPDATE F4801T A INNER JOIN F4801TF B " 
	                                    + "    ON (A.WADOCO = B.WADOCO) " 
	                                    + "   SET A.WADOCO	    = B.WADOCO,      A.WALINE 	  =	B.WALINE,     A.WAMWDH 	 =	B.WAMWDH,  A.WASCSP 	 =	B.WASCSP,"
	                                    + "       A.WASHFT 	    = B.WASHFT,      A.WASRCN     =	B.WASRCN,     A.WALEDG 	 =	B.WALEDG,   A.WACTS4 	 =	B.WACTS4,"
	                                    + "       A.WACTS7 	    = B.WACTS7,      A.WACTS8 	  =	B.WACTS8,     A.WAAID	 =	B.WAAID,    A.WAALSE 	 =	B.WAALSE,"
	                                    + "       A.WAASID 	    = B.WAASID,      A.WAATST 	  =	B.WAATST,     A.WABSEQ 	 =	B.WABSEQ,   A.WACHPR 	 =	B.WACHPR,"
	                                    + "       A.WACRCD 	    = B.WACRCD,      A.WACRCE 	  =	B.WACRCE,     A.WACRCF 	 =	B.WACRCF,   A.WAD5J		 =	B.WAD5J,"
	                                    + "       A.WAD6J		= B.WAD6J,       A.WADRAW 	  =	B.WADRAW,     A.WADUAL 	 =	B.WADUAL,   A.WAMPCE 	 =	B.WAMPCE,"
	                                    + "       A.WAMPRC 	    = B.WAMPRC,      A.WAOBJ	  =	B.WAOBJ,      A.WAOTAM 	 =	B.WAOTAM,   A.WAPRJM 	 =	B.WAPRJM,"
	                                    + "       A.WAPRRP 	    = B.WAPRRP,      A.WASHPP 	  =	B.WASHPP,     A.WASQOR 	 =	B.WASQOR,   A.WASRKF 	 =	B.WASRKF,"
	                                    + "       A.WASRNK 	    = B.WASRNK,      A.WASSOQ 	  =	B.WASSOQ,     A.WATRAF 	 =	B.WATRAF,   A.WAUOM2 	 =	B.WAUOM2,"
	                                    + "       A.WAWR11 	    = B.WAWR11,      A.WAWR12 	  =	B.WAWR12,     A.WAWR13 	 =	B.WAWR13,   A.WAWR14 	 =	B.WAWR14,"
	                                    + "       A.WAWR15 	    = B.WAWR15,      A.WAWR16 	  =	B.WAWR16,     A.WAWR17 	 =	B.WAWR17,   A.WAWR18 	 =	B.WAWR18,"
	                                    + "       A.WAWR19 	    = B.WAWR19,      A.WAWR20 	  =	B.WAWR20,     A.WAJBCD 	 =	B.WAJBCD,   A.WAVFWO 	 =	B.WAVFWO,"
	                                    + "       A.WAPMTN    	= B.WAPMTN,      A.WAISSUE	  =	B.WAISSUE,    A.WAPRODM	 =	B.WAPRODM,  A.WAWHO2 	 =	B.WAWHO2,"
	                                    + "       A.WAAR1		= B.WAAR1,       A.WAPHN1 	  =	B.WAPHN1,     A.WATMCO 	 =	B.WATMCO,   A.WAMTHPR	 =	B.WAMTHPR,"
	                                    + "       A.WAENTCK	    = B.WAENTCK,     A.WARYIN 	  =	B.WARYIN,     A.WARSTM 	 =	B.WARSTM,   A.WACTR		 =	B.WACTR,"
	                                    + "       A.WAREGION 	= B.WAREGION,    A.WATXA1 	  =	B.WATXA1,     A.WAEXR1 	 =	B.WAEXR1,   A.WALNGP 	 =	B.WALNGP,"
	                                    + "       A.WAGLCCV	    = B.WAGLCCV,     A.WAGLCNC	  =	B.WAGLCNC,    A.WACOVGR	 =	B.WACOVGR,  A.WAASN4 	 =	B.WAASN4,"
	                                    + "       A.WAASN2 	    = B.WAASN2,      A.WASEST 	  =	B.WASEST,     A.WASEET 	 =	B.WASEET,   A.WADSAVNAME =	B.WADSAVNAME,"
	                                    + "       A.WATIMEZONES	= B.WATIMEZONES, A.WAPRODF	  =	B.WAPRODF,    A.WACSLPRT =	B.WACSLPRT, A.WAMCUCSL 	 =	B.WAMCUCSL,"
	                                    + "       A.WARLOT 	    = B.WARLOT,      A.WAFAILCD   =	B.WAFAILCD,   A.WAFAILDT =	B.WAFAILDT, A.WAFAILTM 	 =	B.WAFAILTM,"
	                                    + "       A.WAREPDT	    = B.WAREPDT,     A.WAREPTM	  =	B.WAREPTM,    A.WAVEND 	 =	B.WAVEND,   A.WAAN8AS	 =	B.WAAN8AS,"
	                                    + "       A.WAAN8SRM 	= B.WAAN8SRM,    A.WASRYN 	  =	B.WASRYN,     A.WAENTCKS =	B.WAENTCKS, A.WACURBALM1 =	B.WACURBALM1,"
	                                    + "       A.WACURBALM2 	= B.WACURBALM2,  A.WACURBALM3 =	B.WACURBALM3, A.WACRDC   =	B.WACRDC,   A.WACRRM 	 =	B.WACRRM,"
	                                    + "       A.WACRR		= B.WACRR,       A.WAVMRS31   =	B.WAVMRS31,   A.WAVMRS32 =	B.WAVMRS32, A.WASEQN 	 =	B.WASEQN,"
	                                    + "       A.WAPLMR 	    = B.WAPLMR,      A.WAPLLB 	  =	B.WAPLLB,     A.WAPLOS 	 =	B.WAPLOS,   A.WABGTC 	 =	B.WABGTC,"
	                                    + "       A.WATOEM 	    = B.WATOEM,      A.WATOPL 	  =	B.WATOPL,     A.WAPLSA 	 =	B.WAPLSA,   A.WAPLSU 	 =	B.WAPLSU,"
	                                    + "       A.WAESSA 	    = B.WAESSA,      A.WAESSU 	  =	B.WAESSU,     A.WAACSA 	 =	B.WAACSA,   A.WAACSU 	 =	B.WAACSU,"
	                                    + "       A.WAOACM 	    = B.WAOACM,      A.WARACM 	  =	B.WARACM,     A.WAHPLF 	 =	B.WAHPLF,   A.WAVMRS33 	 =	B.WAVMRS33,"
	                                    + "       A.WASCALL	    = B.WASCALL,     A.WAISNO 	  =	B.WAISNO,     A.WARMTHD	 =	B.WARMTHD,  A.WADOC		 =	B.WADOC,"
	                                    + "       A.WADCT		= B.WADCT,       A.WAKCO	  =	B.WAKCO,      A.WACOCH 	 =	B.WACOCH,   A.WALNID 	 =	B.WALNID,"
	                                    + "       A.WACRTU 	    = B.WACRTU,      A.WAXEVT 	  =	B.WAXEVT,     A.WAMCULT	 =	B.WAMCULT,  A.WAWSCHF	 =	B.WAWSCHF,"
	                                    + "       A.WADFMDP	    = B.WADFMDP ");

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
	        stmB = connB.prepareStatement("INSERT INTO F4801T SELECT * FROM F4801TF A WHERE NOT EXISTS(SELECT 'X' FROM F4801T B WHERE A.WADOCO = B.WADOCO)");
	        
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

	        stmB = connB.prepareStatement("DELETE FROM F4801T WHERE NOT EXISTS(SELECT 'X' FROM F4801TF B WHERE WADOCO = B.WADOCO)");
	        
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
