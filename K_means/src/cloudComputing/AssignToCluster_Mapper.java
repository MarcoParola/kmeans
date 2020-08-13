package cloudComputing;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class AssignToCluster_Mapper extends Mapper<LongWritable, Text, IntWritable, Sample>{
	Sample newSample, center; 
	IntWritable clusterIndex;
	private Sample[] centers;
	
	
	public void setup(Context context) throws IOException, InterruptedException
    {
		/*centers[0] = new Sample("1.0 1.0 1.0");
		centers[1] = new Sample("25.0 25.0 25.0");	
		centers[2] = new Sample("104.0 104.0 104.0"); 		TODO DELETE*/
		
		String[] values = context.getConfiguration().getStrings("clusters_centers");
		centers = new Sample[values.length];
		
		for(int i=0; i<values.length; i++)
			centers[i] = new Sample(values[i]);
    }
	
	
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		newSample = new Sample(value.toString());
		double min = Double.MAX_VALUE;
		
		int centersNumber = centers.length;
		for(int i= 0; i < centersNumber; i++) {
			double dist = newSample.computeDistance(centers[i]);
			if(dist < min) {
				min = dist;
				clusterIndex = new IntWritable(i);
			}
		}
		context.write(clusterIndex, newSample);	
	}
}
