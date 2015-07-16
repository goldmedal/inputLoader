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


public class CopyOfInputLoaderAllinMem {
	

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
    	List<String[]> Row = new ArrayList<String[]>();
    	
    	// get the X size
   		xSize = splitRow.length;
   	
   		int i = 0;
   		int j = 0;
   		int check = 0;
   	  	
   		//FSDataOutputStream fsOutStream = hdfs.create(p);1
   		long StartTime = System.currentTimeMillis(); // 取出目前時間
	    while(xBr.ready()){
	    	
	    	// split data
	    	
    		inputBuffer = xBr.readLine();
    		splitRow = inputBuffer.split(",");
    		if(check == 0){ 
    			check  = 1;
    			xSize = splitRow.length;
    		}
    		Row.add(splitRow);
    		
	    }
	    long ProcessTime = System.currentTimeMillis() - StartTime; // 計算處理時間
	    System.out.printf("load time = %d\n", ProcessTime);
	    
	    System.out.println("load over");
   		String outputPath = "/hduser/R/OGA/inputx/data.csv";
   	  	Path p = new Path(outputPath);
	    FSDataOutputStream fsOutStream = hdfs.create(p);
	    
	    StartTime = System.currentTimeMillis(); // 取出目前時間
	    
		for(i=0;i<xSize;i++){

			for(String[] row : Row){
				outputBuffer = outputBuffer + row[i] + ",";
			}
			
			outputBuffer = outputBuffer.substring(0, outputBuffer.length() - 1) +"\n";
			// fw.write(outputBuffer);
			// outputBuffer = "";	
			
		}
		ProcessTime = System.currentTimeMillis() - StartTime; // 計算處理時間
		  System.out.printf("trans time = %d\n", ProcessTime);
		  System.out.println("output over. ready to hdfs");
		  System.out.printf("xSize: %d RowLen: %d\n", xSize, Row.size());
	    xBr.close();
	    xFr.close();
	    
	    byte[] byt = outputBuffer.getBytes();
	    StartTime = System.currentTimeMillis(); // 取出目前時間
	   fsOutStream.write(byt);
	   ProcessTime = System.currentTimeMillis() - StartTime; // 計算處理時間
	   System.out.printf("to hdfs time = %d\n", ProcessTime);
        //hdfs.copyFromLocalFile(src, dst);
		fsOutStream.close();
        hdfs.close();
      //  fw.flush();
      //  fw.close();
        
        System.out.printf("i = %d\n", i);
      
        
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
