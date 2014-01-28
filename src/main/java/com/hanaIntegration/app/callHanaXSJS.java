package com.hanaIntegration.app;

/**
 * Calls a HANA serverside javascript (xsjs)
 *  INPUTS: (mandatory) HANA XSJS URL, output logfile name, username & password 
 *          (optional) n parameters/arguments
 *  OUTPUT: writes HANA XSJS response to a the logfile on HDFS 
 *
 */


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;


public class callHanaXSJS 
{
	public static void main(String[] args) throws IOException 
    {
        //System.out.println( "Hello World!" );
		System.out.println("No. of argumetns are: " + args.length);
	     for(int i= 0;i < args.length;i++) {
	        System.out.println("Argument " + i + " is : " + args[i]);
	     }   
		
		
		String sUrl = args[0];
		//sUrl = "http://earthquake.usgs.gov/earthquakes/feed/csv/2.5/month";
		
		//Append XSJS Command parmeters
		if (args.length > 4) {
			//append first parameter
			sUrl += "?" + args[4];
			//add subsequent
			 for(int i= 5;i < args.length;i++) {
				 sUrl += "&" + args[i];
			 }
		}
		
		System.out.println("HANA XSJS URL is: " + sUrl);
        URL url = new URL(sUrl);
        
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        
        String userpass = args[1] + ":" + args[2];   //args[0] user  args[1] password
        //String userpass = "SYSTEM" + ":" + "Reset1234"; 
        String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());

        conn.setRequestProperty ("Authorization", basicAuth);
        
        conn.connect();
        InputStream connStream = conn.getInputStream();
        
        
        // HDFS Output  
        //java.util.Date date= new java.util.Date();
        //String outputFile = "HANAxsjsResponse " + new Timestamp(date.getTime()) + ".log";
        //new Date().getTime()
        SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMddhhmmss");  //ft.format
        String outputFile = "HANAxsjsResponse " + ft.format(new Date()) + ".log";
        
        FileSystem hdfs = FileSystem.get(new Configuration());
        FSDataOutputStream outStream = hdfs.create(new Path(args[3], outputFile) );

        IOUtils.copy(connStream, outStream);

        outStream.close(); 
        
        
        // System Output (TEST)
        //IOUtils.copy(connStream, System.out); // error using apache class WIP
        //IoUtils.copy(connStream, System.out);
        
        
        connStream.close();
        conn.disconnect();
    }
}

//http://codereview.stackexchange.com/questions/8835/java-most-compact-way-to-print-inputstream-to-system-out
class IoUtils {

    private static final int BUFFER_SIZE = 8192;

    public static long copy(InputStream is, OutputStream os) {
        byte[] buf = new byte[BUFFER_SIZE];
        long total = 0;
        int len = 0;
        try {
            while (-1 != (len = is.read(buf))) {
                os.write(buf, 0, len);
                total += len;
            }
        } catch (IOException ioe) {
            throw new RuntimeException("error reading stream", ioe);
        }
        return total;
    }

}