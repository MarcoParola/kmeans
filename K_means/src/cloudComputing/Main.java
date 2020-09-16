package cloudComputing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	static private final int MAX_ITERATIONS = 100;
	static private final double THRESHOLD = 0.5;
	static Sample[] newCenters, oldCenters;
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Saving start timestamp
		long unixTimeStart = System.currentTimeMillis();

		Configuration conf = new Configuration();

		// Checking number of input arguments and saving them
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
			System.out.println("Wrong args");
			System.exit(2);
	    }

	    int count = 0;
	    Path input = new Path(otherArgs[0]);
	    String output;
	    int numSamples = Integer.parseInt(otherArgs[2]);
	    Integer k = Integer.parseInt(otherArgs[3]);
	    Integer pointDimension = Integer.parseInt(otherArgs[4]);

	    // Initialization of the centers for the first iteration
	    newCenters = initCenters(k, numSamples, otherArgs[0], conf);

	    // Sarting kmeans algorithm
	    while(true) {
	    	count++;
	    	System.out.println("Iteration: " + count);
	    	
	    	output = otherArgs[1] + count;
		    Job job = Job.getInstance(conf, "kmeans");
		    job.setJarByClass(Main.class);
		    job.setMapperClass(AssignToCluster_Mapper.class);
		    job.setCombinerClass(ComputeCenter_Reducer.class);
		    job.setReducerClass(ComputeCenter_Reducer.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Sample.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Sample.class);
	
		    
		    String[] centers = new String[k];
		    
		    for(int i=0; i<k; i++) 
		    	centers[i] = newCenters[i].getAttributeValuesAsString();
		    
		    
		    job.getConfiguration().setStrings("clusters_centers", centers);
		    job.getConfiguration().setInt("pointDimension", pointDimension);
		    
		    FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Waiting for the job to complete
			job.waitForCompletion(true);
			
			// Updating the new centers for the next iteration
		    oldCenters = newCenters;
		    newCenters = readIntermediateCenters(conf, output, k);
		    
		    // Checking stop condition
		    if(count > MAX_ITERATIONS || checkCenters(newCenters, oldCenters)) 
		    	break;
		    
	    } // Ending kmeans algorithm
	    
	    
	    // Saving end timestamp
	    long unixTimeStop = System.currentTimeMillis();
	    
	    // Writing results in local log file
	    String str = (unixTimeStop - unixTimeStart) + "millis, number of iterations: " + count + "\n";
	    for(int i=0; i<k; i++)
	    	str += i + "\t" + newCenters[i].toString() + "\n";
	    
	    
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
				
		double norm = 0;
		
		for(int i=0; i<newCenters.length; i++)			
			norm += newCenters[i].computeDistance(oldCenters[i]);
			
		return (norm < THRESHOLD) ? true : false;
	}
	
	
	static private Sample[] readIntermediateCenters(Configuration conf, String fileName, int k) {
				
		Sample[] cent = new Sample[k];
		
		try {
        FileSystem hdfs;
			hdfs = FileSystem.get(conf);
			
			int i = 0;
			
			String fn = fileName + "/part-r-00000";
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(fn))));
			if(br != null) {
		        String line = br.readLine();
		        
		        while(line != null) {
		        	System.out.println(line);
		        	cent[i] = new Sample(line.split("\t")[1]);
		        	cent[i].setWeight(Integer.parseInt(line.split("\t")[2]));
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
		Random rd = new Random(34231); ;
		
		for (int j = 0; j < k; j++) {
			
			int n = rd.nextInt(num);
			
			while(contains(indexes, n)) {
				System.out.println("Number already extracted");
				n = rd.nextInt(num);
			}
			indexes[j] = n;
			System.out.println(indexes[j]); 
		}
		
		Arrays.parallelSort(indexes);
		
		BufferedReader br;
		try {
	        Path path = new Path(inputFile);
	    	FileSystem hdfs;
			hdfs = FileSystem.get(conf);
	    	FSDataInputStream in = hdfs.open(path);
	        br = new BufferedReader(new InputStreamReader(in));
	
	        int row = 0;
	        int i = 0;
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
	
	
	private static boolean contains(final int[] arr, final int key) {
	    return Arrays.stream(arr).anyMatch(i -> i == key);
	}
}
