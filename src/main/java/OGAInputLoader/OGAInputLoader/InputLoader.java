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
	
	public static void put_data_to_hdfs(String path, String outputData) throws IOException
	{
		Path p = new Path(path);
		byte[] byt = outputData.getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(p);
		fsOutStream.write(byt);
		fsOutStream.close();
	}
	
	
    public static void main( String[] args ) throws IOException
    {
   
    	System.out.println("--------- Program Start -------");
    	long StartTime = System.currentTimeMillis(); // 取出目前時間
    	String inputxPath = "inputx.csv";  // the X data input path
  //  	String inputyPath = "C://Users/Jax/Desktop/inputy.csv"; // the Y data input path
    	String inputBuffer = "";  
    	String outputBuffer = "";
    	int xSize = 0;
   
    	// set Hadoop Configuration
    	
    	set_hadoop();
    	
    	// set FILE Reader
        FileReader xFr = new FileReader(inputxPath);
      //  FileReader yFr = new FileReader(inputyPath);
        BufferedReader xBr = new BufferedReader(xFr);
       // BufferedReader yBr = new BufferedReader(yFr);
        
        FileWriter fw = new FileWriter("data.csv");
        

        // first read
        
    	inputBuffer = xBr.readLine();
    	String[] splitRow = inputBuffer.split(",");
    	
    	// get the X size
   		xSize = splitRow.length;
   		xBr.close();
   		xFr.close();
   		
   		
   		int check = 0;
   		int i = 0;
   		String outputPath = "hdfs://"+hostname+":9000/hduser/R/OGA/inputx/data.csv";
   	  	Path p = new Path(outputPath);
   	 // 	Path dst = new Path("/hduser/R/OGA/inputx/data.csv");
   	 // 	Path src = new Path("data.csv");
   	  	
   	  	
   		FSDataOutputStream fsOutStream = hdfs.create(p);
   		long StartTime1 = System.currentTimeMillis(); // 取出目前時間
        for(i=0;i<xSize;i++){
        	
        	xFr = new FileReader(inputxPath);
        	xBr = new BufferedReader(xFr);
        	
		    while(xBr.ready()){
		    	
		    	// split data
	
	    		inputBuffer = xBr.readLine();
	    		splitRow = inputBuffer.split(",");

		    	outputBuffer = outputBuffer + splitRow[i] + ",";
		    	
		    }
		    
		    outputBuffer = outputBuffer.substring(0, outputBuffer.length() - 1) +"\n";
		    
		    // put data to hdfs
		    
		  
		 //   put_data_to_hdfs(outputPath, outputBuffer);
				

/*			if( (i%500) == 0 ){
				fw.write(outputBuffer);
				outputBuffer = "";
				check = 1;
			}
*/			
		   // System.out.println(outputBuffer);
		    
		    // reset
		    
		   
		    xBr.close();
		    xFr.close();
		    
        }
        long ProcessTime1 = System.currentTimeMillis() - StartTime1; // 計算處理時間
        System.out.printf("load and trans time = %d\n", ProcessTime1);
		byte[] byt = outputBuffer.getBytes();
		
		fsOutStream.write(byt);
		fsOutStream.close();
		
		
    //    hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
    //    fw.flush();
 //       fw.close();
        long ProcessTime = System.currentTimeMillis() - StartTime; // 計算處理時間
        System.out.printf("i = %d\n", i);
        System.out.printf("time = %d\n", ProcessTime);
        
        // write y data
        
 /*       String outputPath = "hdfs://"+hostname+":9000/hduser/R/OGA/inputy/data.csv";
        Path p = new Path(outputPath);
        FSDataOutputStream fsOutStream = hdfs.create(p);
	    while(yBr.ready()){
	    	
    		inputBuffer = yBr.readLine();
	    	byte[] byt = inputBuffer.getBytes();
	    	fsOutStream.write(byt);
	    	
	    */	
//	    }
	    
		//fsOutStream.close();
        //yFr.close();
        
    }

}
