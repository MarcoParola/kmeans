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
public class ComputeCenter_Reducer extends Reducer<IntWritable, Sample, IntWritable, Text>{
	
	Sample newCenter;
	
	public void reduce(IntWritable key, Iterable<Sample> list, Context context) throws IOException, InterruptedException {
		
		newCenter = new Sample(3);
		int count = 0;
		for(Sample s : list) {
			
			double[] temp = newCenter.getAttributeValues();
			
			for(int i=0; i< 3; i++) {
					temp[i] = s.getAttributeValues()[i] + temp[i];
			}
			
			newCenter.setAttributeValues(temp);
			count++;
		}
			
		for(int i=0; i < 3; i++) {
			double temp = newCenter.getAttributeValues()[i]  / count;
			newCenter.getAttributeValues()[i] = temp;
		}
		
		context.write(key, new Text(newCenter.attributeValues[0] + " " + newCenter.attributeValues[1] + " " + newCenter.attributeValues[2]));

	}
}
