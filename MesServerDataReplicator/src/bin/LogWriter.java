package bin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogWriter {

    static BufferedWriter bw = null;
    
    static String path;
    static String decodedPath;
    static String parentPath;
    
    static Date date = new Date();
    static DateFormat dateformat = new SimpleDateFormat("dd.MM.yyyy");
    static DateFormat timeformat = new SimpleDateFormat("HH.mm.ss");
    
	static String datenow;
	static String timenow;
    
	public LogWriter() {

    }
	
	public static void OpenFile(){
		                                     
        datenow = dateformat.format(date);
        timenow = timeformat.format(date);
        
        path = MesDataReplicator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        
        try {
			decodedPath = URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        
        parentPath = new File(decodedPath).getParentFile().getPath();
        parentPath = parentPath + File.separator + "log" + File.separator;
        
		try {
			bw = new BufferedWriter(new FileWriter(parentPath + datenow + "_" + timenow + ".log", true));
	    } catch (IOException ioe) {
	    	 ioe.printStackTrace();
	    }	
	}
	
	public static void write(String data) {
		                             
        datenow = dateformat.format(date);
        timenow = timeformat.format(date);

		try {			
			bw.write(datenow + " " + timenow + " ---> " + data);
			bw.newLine();
			bw.flush();
	    } catch (IOException ioe) {
	    	 ioe.printStackTrace();
	    } 
	}	
	
	public static void CloseFile(){
		
		try {
			bw.close();;
	    } catch (IOException ioe) {
	    	 ioe.printStackTrace();
	    } finally {            
	    	if (bw != null) try {
	    		bw.close();
	    	} catch (IOException e) {
		    
	    	}
	    } 
		
	}
	
}
