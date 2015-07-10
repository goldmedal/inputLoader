package OGAInputLoader.OGAInputLoader;
import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InputLoader {
	

	private static Configuration conf;
	private static FileSystem hdfs;
	private static String hostname = "node1";
	
	public static void set_hadoop() throws IOException
	{
		conf = new Configuration();
    	hdfs = FileSystem.get(conf);
	}
	
	public static void put_data_to_hdfs(int index, String outputData) throws IOException
	{
		Path p = new Path("hdfs://"+hostname+":9000/hduser/R/OGA/input/XData"+index+".csv");
		byte[] byt = outputData.getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(p);
		fsOutStream.write(byt);
		fsOutStream.close();
	}
	
	
    public static void main( String[] args ) throws IOException
    {
   
    	System.out.println("--------- Program Start -------");
    	
    	String inputxPath = "C://Users/Jax/Desktop/inputx.csv";  // the X data input path
    	String inputyPath = "C://Users/Jax/Desktop/inputy.csv"; // the Y data input path
    	String inputBuffer = "";  
    	String outputBuffer = "";
    	int xSize = 0;
   
    	// set Hadoop Configuration
    	
    	// set_hadoop();
    	
    	// set FILE Reader
        FileReader xFr = new FileReader(inputxPath);
        FileReader yFr = new FileReader(inputyPath);
        BufferedReader xBr = new BufferedReader(xFr);
        BufferedReader yBr = new BufferedReader(yFr);

        // first read
        
    	inputBuffer = xBr.readLine();
    	String[] splitRow = inputBuffer.split(",");
    	
    	// get the X size
   		xSize = splitRow.length;
   		xBr.close();
   		xFr.close();
   		
   		int i = 0;
   		
        for(i=0;i<xSize;i++){
        	
        	xFr = new FileReader(inputxPath);
        	xBr = new BufferedReader(xFr);
        	
		    while(xBr.ready()){
		    	
		    	// split data
	
	    		inputBuffer = xBr.readLine();
	    		splitRow = inputBuffer.split(",");

		    	outputBuffer = outputBuffer + splitRow[i] + ",";
		    	
		    }
		    
		    outputBuffer = outputBuffer.substring(0, outputBuffer.length() - 1);
		    
		    // put data to hdfs
		    put_data_to_hdfs(i, outputBuffer);
		    
		   // System.out.println(outputBuffer);
		    
		    // reset
		    
		    outputBuffer = "";
		    xBr.close();
		    xFr.close();
		    
        }
        	
       
        yFr.close();
        
    }

}
