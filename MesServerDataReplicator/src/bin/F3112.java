package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class F3112 {

	public F3112() {
		
	}
	
    public void executeUpdate(Connection connB) {
		
		PreparedStatement stmB = null;
		        
		LogWriter.write("Aggiornamento dati modificati tabella: F2112 - [ Cicli Ordini di Produzione ]");
		
		System.out.println("");
        System.out.println("Aggiornamento dati modificati tabella: F2112 - [ Cicli Ordini di Produzione ]");
        
        try {
	        stmB = connB.prepareStatement("UPDATE F3112 A INNER JOIN F3112F B " 
	                                    + "    ON (A.WLDOCO = B.WLDOCO AND A.WLMCU = B.WLMCU AND A.WLOPSQ = B.WLOPSQ AND A.WLOPSC = B.WLOPSC) " 
	                                    + "   SET A.WLDOCO 	= B.WLDOCO,  A.WLDCTO =	B.WLDCTO, A.WLSFXO 	  =	B.WLSFXO,    A.WLTRT	 = B.WLTRT,"
	                                    + "       A.WLKIT	= B.WLKIT,   A.WLKITL =	B.WLKITL, A.WLKITA 	  =	B.WLKITA,    A.WLMMCU 	 = B.WLMMCU,"
	                                    + "       A.WLLINE 	= B.WLLINE,  A.WLMCU  =	B.WLMCU,  A.WLALD	  =	B.WLALD,     A.WLDSC1 	 = B.WLDSC1,"
	                                    + "       A.WLOPSQ 	= B.WLOPSQ,  A.WLOPSC =	B.WLOPSC, A.WLOPST	  =	B.WLOPST,    A.WLINPE 	 = B.WLINPE,"
	                                    + "       A.WLTIMB 	= B.WLTIMB,  A.WLLAMC =	B.WLLAMC, A.WLBFPF 	  =	B.WLBFPF,    A.WLPPRF 	 = B.WLPPRF,"
	                                    + "       A.WLJBCD 	= B.WLJBCD,  A.WLAN8  =	B.WLAN8,  A.WLCRTR 	  =	B.WLCRTR,    A.WLSLTR 	 = B.WLSLTR,"
	                                    + "       A.WLTRDJ 	= B.WLTRDJ,  A.WLDRQJ =	B.WLDRQJ, A.WLSTRT 	  =	B.WLSTRT,    A.WLSTRX 	 = B.WLSTRX,"
	                                    + "       A.WLRSFT 	= B.WLRSFT,  A.WLSSFT =	B.WLSSFT, A.WLCSFT 	  =	B.WLCSFT,    A.WLLTPC 	 = B.WLLTPC,"
	                                    + "       A.WLPOVR 	= B.WLPOVR,  A.WLOPYP =	B.WLOPYP, A.WLCPYP 	  =	B.WLCPYP,    A.WLNXOP 	 = B.WLNXOP,"
	                                    + "       A.WLSETC 	= B.WLSETC,  A.WLMOVD =	B.WLMOVD, A.WLQUED 	  =	B.WLQUED,    A.WLRUNM 	 = B.WLRUNM,"
	                                    + "       A.WLRUNL 	= B.WLRUNL,  A.WLSETL =	B.WLSETL, A.WLMACA 	  =	B.WLMACA,    A.WLLABA 	 = B.WLLABA,"
	                                    + "       A.WLSETA 	= B.WLSETA,  A.WLOPSR =	B.WLOPSR, A.WLUORG 	  =	B.WLUORG,    A.WLSOCN 	 = B.WLSOCN,"
	                                    + "       A.WLSOQS 	= B.WLSOQS,  A.WLQMTO =	B.WLQMTO, A.WLPWRT 	  =	B.WLPWRT,    A.WLUOM 	 = B.WLUOM,"
	                                    + "       A.WLCTS2 	= B.WLCTS2,  A.WLCTS3 =	B.WLCTS3, A.WLCTS4 	  =	B.WLCTS4,    A.WLCTS5 	 = B.WLCTS5,"
	                                    + "       A.WLCTS6 	= B.WLCTS6,  A.WLAPID =	B.WLAPID, A.WLSHNO 	  =	B.WLSHNO,    A.WLOMCU 	 = B.WLOMCU,"
	                                    + "       A.WLOBJ	= B.WLOBJ,   A.WLSUB  =	B.WLSUB,  A.WLVEND 	  =	B.WLVEND,    A.WLRKCO 	 = B.WLRKCO,"
	                                    + "       A.WLRORN 	= B.WLRORN,  A.WLRCTO =	B.WLRCTO, A.WLRLLN 	  =	B.WLRLLN,    A.WLDCT 	 = B.WLDCT,"
	                                    + "       A.WLURCD 	= B.WLURCD,  A.WLURDT =	B.WLURDT, A.WLURAT 	  =	B.WLURAT,    A.WLURRF 	 = B.WLURRF,"
	                                    + "       A.WLURAB 	= B.WLURAB,  A.WLUSER =	B.WLUSER, A.WLPID	  =	B.WLPID,     A.WLJOBN 	 = B.WLJOBN,"
	                                    + "       A.WLUPMJ 	= B.WLUPMJ,  A.WLTDAY =	B.WLTDAY, A.WLLOCN 	  =	B.WLLOCN,    A.WLRUC	 = B.WLRUC,"
	                                    + "       A.WLCAPU	= B.WLCAPU,  A.WLWMCU =	B.WLWMCU, A.WLCMHR 	  =	B.WLCMHR,    A.WLCLHR 	 = B.WLCLHR,"
	                                    + "       A.WLCSHR 	= B.WLCSHR,  A.WLCOST =	B.WLCOST, A.WLACTB 	  =	B.WLACTB,    A.WLNUMB 	 = B.WLNUMB,"
	                                    + "       A.WLCTS7 	= B.WLCTS7,  A.WLCTS8 =	B.WLCTS8, A.WLCTS9 	  =	B.WLCTS9,    A.WLLABP 	 = B.WLLABP,"
	                                    + "       A.WLMACR 	= B.WLMACR,  A.WLSETR =	B.WLSETR, A.WLAPSC 	  =	B.WLAPSC,    A.WLSEST 	 = B.WLSEST,"
	                                    + "       A.WLSEET 	= B.WLSEET,  A.WLTMCO =	B.WLTMCO, A.WLD2J	  =	B.WLD2J,     A.WLSTTI 	 = B.WLSTTI,"
	                                    + "       A.WLCMPE 	= B.WLCMPE,  A.WLCMPC =	B.WLCMPC, A.WLCPLVLFR =	B.WLCPLVLFR, A.WLCPLVLTO = B.WLCPLVLTO,"
	                                    + "       A.WLCMRQ 	= B.WLCMRQ,  A.WLANSA =	B.WLANSA, A.WLANPA 	  =	B.WLANPA,    A.WLANP	 = B.WLANP,"
	                                    + "       A.WLWSCHF	= B.WLWSCHF, A.WLTRAF =	B.WLTRAF "
	                                    + " WHERE CONCAT(A.WLUPMJ,A.WLTDAY) < CONCAT(B.WLUPMJ,B.WLTDAY)");

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
	        stmB = connB.prepareStatement("INSERT INTO F3112 SELECT * FROM F3112F A WHERE NOT EXISTS(SELECT 'X' FROM F3112 B WHERE A.WLDOCO = B.WLDOCO AND A.WLMCU = B.WLMCU AND A.WLOPSQ = B.WLOPSQ AND A.WLOPSC = B.WLOPSC)");
	        
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

	        stmB = connB.prepareStatement("DELETE FROM F3112 WHERE NOT EXISTS(SELECT 'X' FROM F3112F B WHERE WLDOCO = B.WLDOCO AND WLMCU = B.WLMCU AND WLOPSQ = B.WLOPSQ AND WLOPSC = B.WLOPSC)");
	        
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
