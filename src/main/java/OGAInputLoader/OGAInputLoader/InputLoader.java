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
	
    public static void main( String[] args ) throws IOException
    {
    	String inputxPath = "inputx.csv";
    	String inputyPath = "inputy.csv";
    	
    	// FILE Read
        FileReader xFr = new FileReader(inputxPath);
        FileReader yFr = new FileReader(inputyPath);
        
        xFr.close();
        yFr.close();
        
    }

}
