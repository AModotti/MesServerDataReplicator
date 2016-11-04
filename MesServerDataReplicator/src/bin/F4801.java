package bin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class F4801 {
	
	public F4801() {
		
	}
	
    public void executeUpdate(Connection connB) {
		
		PreparedStatement stmB = null;
		        
		LogWriter.write("Aggiornamento dati modificati tabella: F4801 - [ Testate Ordini di Produzione 1 ]");
		
		System.out.println("");
        System.out.println("Aggiornamento dati modificati tabella: F4801 - [ Testate Ordini di Produzione 1 ]");

        try {
	        stmB = connB.prepareStatement("UPDATE F4801 A INNER JOIN F4801F B " 
	                                    + "    ON (A.WADOCO = B.WADOCO) " 
	                                    + "   SET A.WADCTO = B.WADCTO, A.WASFXO	= B.WASFXO, A.WARCTO = B.WARCTO, A.WARORN =	B.WARORN, A.WALNID = B.WALNID,"
	                                    + "       A.WAPTWO = B.WAPTWO, A.WAPARS	= B.WAPARS, A.WATYPS = B.WATYPS, A.WAPRTS =	B.WAPRTS, A.WADL01 = B.WADL01,"
	                                    + "       A.WASTCM = B.WASTCM, A.WACO	= B.WACO,   A.WAMCU  = B.WAMCU,  A.WAMMCU =	B.WAMMCU, A.WALOCN = B.WALOCN,"
	                                    + "       A.WAAISL = B.WAAISL, A.WABIN 	= B.WABIN,  A.WASRST = B.WASRST, A.WADCG  =	B.WADCG,  A.WASUB  = B.WASUB,"
	                                    + "       A.WAAN8  = B.WAAN8,  A.WAANO 	= B.WAANO,  A.WAANSA = B.WAANSA, A.WAANPA =	B.WAANPA, A.WAANP  = B.WAANP,"
	                                    + "       A.WADPL  = B.WADPL,  A.WAANT 	= B.WAANT,  A.WANAN8 = B.WANAN8, A.WATRDJ =	B.WATRDJ, A.WASTRT = B.WASTRT,"
	                                    + "       A.WADRQJ = B.WADRQJ, A.WASTRX	= B.WASTRX, A.WADAP  = B.WADAP,  A.WADAT  =	B.WADAT,  A.WAPPDT = B.WAPPDT,"
	                                    + "       A.WAWR01 = B.WAWR01, A.WAWR02	= B.WAWR02, A.WAWR03 = B.WAWR03, A.WAWR04 =	B.WAWR04, A.WAWR05 = B.WAWR05,"
	                                    + "       A.WAWR06 = B.WAWR06, A.WAWR07	= B.WAWR07, A.WAWR08 = B.WAWR08, A.WAWR09 =	B.WAWR09, A.WAWR10 = B.WAWR10,"
	                                    + "       A.WAVR01 = B.WAVR01, A.WAVR02	= B.WAVR02, A.WAAMTO = B.WAAMTO, A.WASETC =	B.WASETC, A.WABRT  = B.WABRT,"
	                                    + "       A.WAPAYT = B.WAPAYT, A.WAAMTC	= B.WAAMTC, A.WAHRSO = B.WAHRSO, A.WAHRSC =	B.WAHRSC, A.WAAMTA = B.WAAMTA,"
	                                    + "       A.WAHRSA = B.WAHRSA, A.WAITM 	= B.WAITM,  A.WAAITM = B.WAAITM, A.WALITM =	B.WALITM, A.WANUMB = B.WANUMB,"
	                                    + "       A.WAAPID = B.WAAPID, A.WAUORG	= B.WAUORG, A.WASOBK = B.WASOBK, A.WASOCN =	B.WASOCN, A.WASOQS = B.WASOQS,"
	                                    + "       A.WAQTYT = B.WAQTYT, A.WAUOM 	= B.WAUOM,  A.WASHNO = B.WASHNO, A.WAPBTM =	B.WAPBTM, A.WATBM  = B.WATBM,"
	                                    + "       A.WATRT  = B.WATRT,  A.WASHTY	= B.WASHTY, A.WAPEC  = B.WAPEC,  A.WAPPFG =	B.WAPPFG, A.WABM   = B.WABM,"
	                                    + "       A.WARTG  = B.WARTG,  A.WASPRT	= B.WASPRT, A.WAUNCD = B.WAUNCD, A.WAINDC =	B.WAINDC, A.WARESC = B.WARESC,"
	                                    + "       A.WAMOH  = B.WAMOH,  A.WATDT 	= B.WATDT,  A.WAPOU  = B.WAPOU,  A.WAPC	  =	B.WAPC,   A.WALTLV = B.WALTLV,"
	                                    + "       A.WALTCM = B.WALTCM, A.WACTS1	= B.WACTS1, A.WALOTN = B.WALOTN, A.WALOTP =	B.WALOTP, A.WALOTG = B.WALOTG,"
	                                    + "       A.WARAT1 = B.WARAT1, A.WARAT2	= B.WARAT2, A.WADCT  = B.WADCT,  A.WASBLI =	B.WASBLI, A.WARKCO = B.WARKCO,"
	                                    + "       A.WABREV = B.WABREV, A.WARREV	= B.WARREV, A.WADRWC = B.WADRWC, A.WARTCH =	B.WARTCH, A.WAPNRQ = B.WAPNRQ,"
	                                    + "       A.WAREAS = B.WAREAS, A.WAPHSE	= B.WAPHSE, A.WAXDSP = B.WAXDSP, A.WABOMC =	B.WABOMC, A.WAURCD = B.WAURCD,"
	                                    + "       A.WAURDT = B.WAURDT, A.WAURAT	= B.WAURAT, A.WAURAB = B.WAURAB, A.WAURRF =	B.WAURRF, A.WAUSER = B.WAUSER,"
	                                    + "       A.WAPID  = B.WAPID,  A.WAJOBN	= B.WAJOBN, A.WAUPMJ = B.WAUPMJ, A.WATDAY =	B.WATDAY, A.WAAAID = B.WAAAID,"
	                                    + "       A.WANTST = B.WANTST, A.WAXRTO	= B.WAXRTO, A.WAESDN = B.WAESDN, A.WAACDN =	B.WAACDN, A.WASAID = B.WASAID,"
	                                    + "       A.WAMPOS = B.WAMPOS, A.WAAPRT	= B.WAAPRT, A.WAAMLC = B.WAAMLC, A.WAAMMC =	B.WAAMMC, A.WAAMOT = B.WAAMOT,"
	                                    + "       A.WALBAM = B.WALBAM, A.WAMTAM	= B.WAMTAM "
	                                    + " WHERE CONCAT(A.WAUPMJ,A.WATDAY) < CONCAT(B.WAUPMJ,B.WATDAY)");

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
	        stmB = connB.prepareStatement("INSERT INTO F4801 SELECT * FROM F4801F A WHERE NOT EXISTS(SELECT 'X' FROM F4801 B WHERE B.WADOCO = A.WADOCO)");
	        
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

	        stmB = connB.prepareStatement("DELETE FROM F4801 WHERE NOT EXISTS(SELECT 'X' FROM F4801F B WHERE WADOCO = B.WADOCO)");
	        
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
