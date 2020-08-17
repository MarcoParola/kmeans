package cloudComputing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;

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

import com.nimbusds.jose.util.ArrayUtils;
import com.squareup.okhttp.internal.io.*;

public class Main {

	static private final int MAX_ITERATIONS = 30;
	static private final double THRESHOLD = 0.5;
	static private String[] dimClusters;
	static Sample[] newCenters, oldCenters; // TODO INIZIALIZZA
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		long unixTimeStart = System.currentTimeMillis();
		
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
	    dimClusters = new String[k];
	    Integer numReducers = Integer.parseInt(otherArgs[4]);
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
		    job.setNumReduceTasks(numReducers);
	
		    
		    String[] centers = new String[k];
		    
		    for(int i=0; i<k; i++) {
		    	centers[i] = newCenters[i].toString();
		    }
		    
		    job.getConfiguration().setStrings("clusters_centers", centers);
		    job.getConfiguration().setInt("numReducers", numReducers);
		    
		    FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, new Path(output));
	
			job.waitForCompletion(true);
			
			
		    oldCenters = newCenters;
		    newCenters = readIntermediateCenters(conf, output, k, numReducers);
		    
		    
		    
		    if(count > MAX_ITERATIONS || checkCenters(newCenters, oldCenters)) {
		    	break;
		    	// TODO scrivi risultati finali??
		    }
	    }
	    
	    long unixTimeStop = System.currentTimeMillis();
	    
	    
	    String str = (unixTimeStop - unixTimeStart) + " millis, number of iterations: " + count + "\n";
	    for(int i=0; i<k; i++)
	    	str += i + "\t" + newCenters[i].toString() + "\t, dimCluster: " + dimClusters[i] + "\n";
	    
	    
	    
	    BufferedWriter out = null;

	    try {
	        FileWriter fstream = new FileWriter("out/output" + input, true); 
	        out = new BufferedWriter(fstream);
	        out.write(str);
	    }

	    catch (IOException e) {
	        System.err.println("Error: " + e.getMessage());
	    }

	    finally {
	        if(out != null) {
	            out.close();
	        }
	    }
	    
	}
	
	
	static private boolean checkCenters(Sample[] newCenters, Sample[] oldCenters) {
		
		// TODO implementa interfaccia comparable
		
		double norm = 0;
		
		for(int i=0; i<newCenters.length; i++)			
			norm += newCenters[i].computeDistance(oldCenters[i]);
			
		return (norm < THRESHOLD) ? true : false;
	}
	
	
	static private Sample[] readIntermediateCenters(Configuration conf, String fileName, int k, int numRed) {
		
		
		System.out.println("--------- numero di reducer: " + numRed);
		
		Sample[] cent = new Sample[k];
		
		
		try {
        FileSystem hdfs;
			hdfs = FileSystem.get(conf);
			// TODO così potrebbe non funzionare, perchè potrei avere meno cluster, nel caso in cui ad un centroide non siano 
			// stati assegnati sample

			// gestione lettura dei centri, considerando che potrebbero essere stati scritti da diversi reducer al passo precedente
			// con j scorro i file
			int i = 0;
			for(int j=0; j<numRed; j++) {
			
				String fn = (j < 10) ? fileName + "/part-r-0000" + j : fileName + "/part-r-000" + j;
		        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(fn))));
		        String line = br.readLine();
		        
		        while(line != null) {
		        	System.out.println(line);
		        	cent[i] = new Sample(line.split("\t")[1]);
		        	dimClusters[i] = line.split("\t")[2];
		        	i++;
		        	line = br.readLine();
		        }
		        
		        br.close();
			}
	        
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return cent;
	}
	
	static private Sample[] initCenters(int k, int num, String inputFile, Configuration conf) {
		Sample[] cent = new Sample[k];
		int[] indexes = new int[k];
		Random rd = new Random(); 
		
		for (int j = 0; j < k; j++) {
			
			int n = rd.nextInt(num);
			
			while(contains(indexes, n)) {
				
				System.out.println("number alrady extract");
				n = rd.nextInt(num);
			}
			indexes[j] = n;
			System.out.println(indexes[j]); 
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
	
	public static boolean contains(final int[] arr, final int key) {
	    return Arrays.stream(arr).anyMatch(i -> i == key);
	}
}
