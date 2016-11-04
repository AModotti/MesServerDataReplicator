package bin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MesDataReplicator {

	public static void main(String[] args) {

		String ip = ""; 
	    String mysqldbusername = ""; 
	    String mysqldbpassword = ""; 
	    
	    DecimalFormat dfs = new DecimalFormat("#");
	    DecimalFormat dfm = new DecimalFormat("#.##");
	    
        LogWriter.OpenFile();
        
	    if(args.length == 0)
	    {
	    	LogWriter.write("Errore: nessun parametro di lancio impostato");
	    	LogWriter.write("Utilizzo: MesDataReplicator.jar [Indirizzo Ip MySql Server] [Username] [Password]");
	        
	        System.out.println("Errore: nessun parametro di lancio impostato");
	        System.out.println("Utilizzo: MesDataReplicator.jar [Indirizzo Ip MySql Server] [Username] [Password]");
	        
	        LogWriter.CloseFile();
	        System.exit(0);
	    }else{
	    	ip = args[0];
	    	mysqldbusername = args[1];
	    	mysqldbpassword = args[2];
	    }
	    		
		String starttimestamp;
		String endtimestamp;
		Date date;
		
		String oracledbusername = "PRODDTA"; 		  
	    String oracledbpassword = "PRODDTA";
 
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}

		
        String oracledbconnectionstring  = "jdbc:oracle:thin:@192.168.2.12:1521:E1DATA";
        String mysqldbconnectionstring  = "jdbc:mysql://" + ip + ":3306/MES?useSSL=true";
		
		Connection connA = null;
		Connection connB = null;
		
		try {
			connA = DriverManager.getConnection(oracledbconnectionstring,oracledbusername,oracledbpassword);
			connB = DriverManager.getConnection(mysqldbconnectionstring,mysqldbusername,mysqldbpassword);
		} catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
               
        DateFormat dateformat = new SimpleDateFormat("dd/MM/yyyy HH.mm.ss");
        
        date = new Date(); 
        starttimestamp = dateformat.format(date);
                
        LogWriter.write("***** Mes Database Synchronizer [ from: Oracle to: Mysql ] *****");
    	LogWriter.write("Indirizzo ip di destinazione: " + ip);
        LogWriter.write("Inizio procedura di sincronizzazione " + "[ " + starttimestamp + " ]");
        
		System.out.println("***** Mes Database Synchronizer [ from: Oracle to: Mysql ] *****");
		System.out.println("");
		System.out.println("Indirizzo ip di destinazione: " + ip);
		System.out.println("");
		System.out.println("Inizio procedura di sincronizzazione " + "[ " + starttimestamp + " ]");
		
//***** INIZIO IMPORTAZIONE DATI SU TABELLA DI FRONTIERA ********************************************************************************************
		
        F0006F F0006F = new F0006F();
        F0101F F0101F = new F0101F();
        F4801F F4801F = new F4801F();
        F4801TF F4801TF = new F4801TF();
        F3112F F3112F = new F3112F();
        
		F0006F.executeImport(connA, connB);
		F0101F.executeImport(connA, connB);
		F4801F.executeImport(connA, connB);
		F4801TF.executeImport(connA, connB);
		F3112F.executeImport(connA, connB);
      
//***** INIZIO PROCEDURA DI ELABORAZIONE VARIAZIONI **********************************************************************************
        
		LogWriter.write("Inizio procedura di elaborazione variazioni...");
		System.out.println("");
		System.out.println("Inizio procedura di elaborazione variazioni...");
		
        F0006 F0006 = new F0006();
		F0006.executeUpdate(connB);
		F0006.executeInsert(connB);
        F0006.executeDelete(connB);

        F0101 F0101 = new F0101();
    	F0101.executeUpdate(connB);
    	F0101.executeInsert(connB);
    	F0101.executeDelete(connB);
		
        F4801 F4801 = new F4801();
        F4801.executeUpdate(connB);
        F4801.executeInsert(connB);
        F4801.executeDelete(connB);
        
        F4801T F4801T = new F4801T();
        F4801T.executeUpdate(connB);
        F4801T.executeInsert(connB);
        F4801T.executeDelete(connB);
                
        F3112 F2112 = new F3112();
        F2112.executeUpdate(connB);
        F2112.executeInsert(connB);
        F2112.executeDelete(connB);
              
//***** FINE PROCEDURA ******************************************************************************************************************************
        
        date = new Date(); 
        endtimestamp = dateformat.format(date);

        LogWriter.write("Fine procedura di elaborazione variazioni");
		System.out.println("");
		System.out.println("Fine procedura di elaborazione variazioni");
		
        LogWriter.write("Fine procedura di sincronizzazione" + " [ " + endtimestamp + " ]");
        System.out.println("");
        System.out.println("Fine procedura di sincronizzazione" + " [ " + endtimestamp + " ]");
        
        double diff = 0;
        double diffs = 0;
        double diffm = 0;
        
		try {
			diff = (dateformat.parse(endtimestamp).getTime() - dateformat.parse(starttimestamp).getTime());
		} catch (ParseException e) {
			LogWriter.write(e.getMessage());
			e.printStackTrace();
		}
        
        diffs = diff / 1000;
        diffm = diff / 1000 / 60;
        
        LogWriter.write("Durata procedura in secondi: " + " [ " + dfs.format(diffs) + " ]");
        LogWriter.write("Durata procedura in minuti: " + " [ " + dfm.format(diffm) + " ]");
        
        System.out.println("");
        System.out.println("Durata procedura in secondi: " + " [ " + dfs.format(diffs) + " ]");
        System.out.println("Durata procedura in minuti: " + " [ " + dfm.format(diffm) + " ]");

        try {
			connA.close();
		} catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
        try {
			connB.close();
		} catch (SQLException e) {
			LogWriter.write("Errore: " + e.getMessage());
			e.printStackTrace();
		}
        
        LogWriter.CloseFile();

	}
	
}
