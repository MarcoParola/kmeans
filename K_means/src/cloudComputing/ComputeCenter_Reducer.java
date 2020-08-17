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
		
		int size = list.iterator().next().getAttributeValues().length;
		
		newCenter = new Sample(size);
		int count = 0;
		for(Sample s : list) {
			
			double[] temp = newCenter.getAttributeValues();
			
			for(int i=0; i< size; i++) {
					temp[i] = s.getAttributeValues()[i] + temp[i];
			}
			
			newCenter.setAttributeValues(temp);
			count++;
		}
			
		for(int i=0; i < size; i++) {
			double temp = newCenter.getAttributeValues()[i]  / count;
			newCenter.getAttributeValues()[i] = temp;
		}
		
		String val = "";
		for(int i=0; i<newCenter.attributeValues.length; i++)
			val += newCenter.attributeValues[i] + " ";
		
		context.write(key, new Text(val));

	}
}
