package cloudComputing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.protobuf.ByteString.Output;

import org.apache.hadoop.mapreduce.Mapper.Context;

//public class ComputeCenter_Reducer extends Reducer<IntWritable, Sample, IntWritable, Sample>{
public class Combiner extends Reducer<IntWritable, Sample, IntWritable, Sample>{
	
	Sample newCenter;
	
	public void reduce(IntWritable key, Iterable<Sample> list, Context context) throws IOException, InterruptedException {
		
		int size = list.iterator().next().getAttributeValues().length;
		
		newCenter = new Sample(size);
		double[] temp = newCenter.getAttributeValues();

		int count = 0;
		
		for(Sample s : list) {
			
			double[] point = s.getAttributeValues();
			int w = s.getWeight();
			for(int i=0; i < size; i++) {
				temp[i] += point[i] * w;
			}
			count += w;
		}
			
		for(int i=0; i < size; i++) 
			temp[i] /= count;
		
		newCenter.setAttributeValues(temp);
		newCenter.setWeight(count);
		
		context.write(key, newCenter);

	}
}
