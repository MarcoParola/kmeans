package cloudComputing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.squareup.okhttp.internal.io.*;

public class Main {

	static private final int MAX_ITERATIONS = 30;
	static private final double THRESHOLD = 0.2;
	
	static Sample[] newCenters, oldCenters; // TODO INIZIALIZZA
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: Main <in> [<in>...] <out>"); // TODO specify args
	      System.exit(2);
	    }
	    
	    
	    // print arguments
	    for(int i=0; i<args.length; i++)
	    	System.out.println(args[i]);
	    
	    
	    int count = 0;
	    Integer k = Integer.parseInt(otherArgs[1]);
	    Path input = new Path(otherArgs[0]);
	    int numSamples = Integer.parseInt(otherArgs[3]);
	    String output;
	    newCenters = initCenters(k, numSamples, otherArgs[0], conf);
	    
	    for(Sample s : newCenters)
	    	System.out.println(s.toString());
	    
	    while(true) {
	    	count++;
	    	System.out.println("iterazione" + count);
	    	
	    	output = otherArgs[2] + count;
	    	
		    Job job = Job.getInstance(conf, "kmeans");
		    job.setJarByClass(Main.class);
		    job.setMapperClass(AssignToCluster_Mapper.class);
		    //job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(ComputeCenter_Reducer.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Sample.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Sample.class);
	
		    
		    String[] centers = new String[k];
		    
		    for(int i=0; i<k; i++) {
		    	centers[i] = newCenters[i].toString();
		    }
		    
		    job.getConfiguration().setStrings("clusters_centers", centers);
		    
		    FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, new Path(output));
	
			job.waitForCompletion(true);
			
		    oldCenters = newCenters;
		    newCenters = readIntermediateCenters(conf, output, k);
		    
		    if(count > MAX_ITERATIONS || checkCenters(newCenters, oldCenters)) {
		    	break;
		    	// TODO scrivi risultati finali??
		    }
	    }
	
	}
	
	
	static private boolean checkCenters(Sample[] newCenters, Sample[] oldCenters) {
		
		// TODO implementa interfaccia comparable
		
		double norm = 0;
		
		for(int i=0; i<newCenters.length; i++) {
			for(int j=0; j<newCenters[i].getAttributeValues().length; j++)
			norm += Math.pow(newCenters[i].getAttributeValues()[j] - oldCenters[i].getAttributeValues()[j], 2);
		}
		
		norm = Math.sqrt(norm);
		return (norm < THRESHOLD) ? true : false;
	}
	
	
	static private Sample[] readIntermediateCenters(Configuration conf, String fileName, int k) {
		
		Sample[] cent = new Sample[k];
		
		
		try {
        FileSystem hdfs;
			hdfs = FileSystem.get(conf);
			// TODO così potrebbe non funzionare, perchè potrei avere meno cluster, nel caso in cui ad un centroide non siano 
			// stati assegnati sample
			
	        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(fileName + "/part-r-00000"))));
	        String line = br.readLine();
	        int i = 0;
	        while(line != null) {
	        	System.out.println(line);
	        	cent[i] = new Sample(line.split("\t")[1]);
	        	
	        	i++;
	        	line = br.readLine();
	        }
	        
	        br.close();
	        
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return cent;
	}
	
	static private Sample[] initCenters(int k, int num, String inputFile, Configuration conf) {
		Sample[] cent = new Sample[k];
		int[] indexes = new int[k];
		Random rd = new Random(); 
		
		for (int i = 0; i < k; i++) {
			
			int n = rd.nextInt(num);
			
			while(Arrays.asList(indexes).contains(n)) {
				System.out.println("number alrady extract");
				n = rd.nextInt(num);
			}
			indexes[i] = n;
			System.out.println(indexes[i]); 
		}
		
		Arrays.parallelSort(indexes);
		
		BufferedReader br;
		try {
			//File reading utils
	        Path path = new Path(inputFile);
	    	FileSystem hdfs;
			hdfs = FileSystem.get(conf);
	    	FSDataInputStream in = hdfs.open(path);
	        br = new BufferedReader(new InputStreamReader(in));
	
	        //Get centroids from the file
	        int row = 0;
	        int i = 0;
	        int position;
	        while(i < k) {
	            String sample = br.readLine();
	            if(row == indexes[i]) {    
	                cent[i] = new Sample(sample);  
	                i++;
	            }
	            row++;
	        }  
	        br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cent;
	}
}
