package cloudComputing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

import com.squareup.okhttp.internal.io.FileSystem;

public class Main {

	static private final int MAX_ITERATIONS = 30;
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
	    
	    
	    Integer k = Integer.parseInt(otherArgs[otherArgs.length - 2]);
	    
	    int count = 0;
	    newCenters = initCenters(k);
	    Path input = new Path(otherArgs[otherArgs.length - 3]);
	    String output;
	    
	    while(true) {
	    	count++;
	    	System.out.println("iterazione" + count);
	    	
	    	output = otherArgs[otherArgs.length - 1] + count;
	    	
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
		    
		    for(int i=0; i<k; i++)
		    	centers[i] = newCenters[i].toString();
		    
		    job.getConfiguration().setStrings("clusters_centers", centers);
		    
		    FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, new Path(output));
	
		    //System.exit(job.waitForCompletion(true) ? 0 : 1);
			job.waitForCompletion(true);
			
		    oldCenters = newCenters;
   
		    newCenters = readIntermediateCenters(output, k);
		    
		    if(count > MAX_ITERATIONS || checkCenters(newCenters, oldCenters)) {
		    	break;
		    	// TODO scrivi risultati finali??
		    }
	    }
	
	}
	
	
	static private boolean checkCenters(Sample[] newCenters, Sample[] oldCenters) {
		
		// TODO implementa interfaccia comparable
		return true;
	}
	
	
	static private Sample[] readIntermediateCenters(String fileName, int k) {
		
		Sample[] cent = new Sample[k];
		
		
		cent[0] = new Sample("1.0 1.0 1.0");
		cent[1] = new Sample("25.0 25.0 25.0");	
		cent[2] = new Sample("104.0 104.0 104.0");
		
		
		
		
		
		
		
		
		/*
		
		
		
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));	
        
        for (int i = 0; i < status.length; i++) {
            //Read the centroids from the hdfs
            if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String[] keyValueSplit = br.readLine().split("\t"); //Split line in K,V
                int centroidId = Integer.parseInt(keyValueSplit[0]);
                String[] point = keyValueSplit[1].split(",");
                points[centroidId] = new Point(point);
                br.close();
            }
        }
        //Delete temp directory
        hdfs.delete(new Path(pathString), true); 

    	return points;
		
		
		*/
		
		
		
		
		
		// apri il file fileName e leggi il contenuto
		
		// elimina il contenuto di fileName
		
		return cent;
	}
	
	static private Sample[] initCenters(int k) {
		Sample[] cent = new Sample[k];
		
		// TODO estraii con qualche criterio
		
		cent[0] = new Sample("1.0 1.0 1.0");
		cent[1] = new Sample("25.0 25.0 25.0");	
		cent[2] = new Sample("104.0 104.0 104.0");
		
		return cent;
	}
}
