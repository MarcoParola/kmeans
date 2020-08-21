package cloudComputing;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ComputeCenter_Reducer extends Reducer<IntWritable, Sample, IntWritable, Sample>{
	
	Sample newCenter;
	
	public void reduce(IntWritable key, Iterable<Sample> list, Context context) throws IOException, InterruptedException {
		
		int size = context.getConfiguration().getInt("pointDimension", 1);
		
		newCenter = new Sample(size);
		double[] temp = newCenter.getAttributeValues();
		
		int count = 0;
		
		for(Sample s : list) {
			
			double[] point = s.getAttributeValues();
			int w = s.getWeight();
			
			for(int i=0; i< size; i++)
				temp[i] += point[i] * w;
			
			count += w;
		}
			
		for(int i=0; i < size; i++) 
			temp[i] /= count;
		
		newCenter.setAttributeValues(temp);
		newCenter.setWeight(count);
		
		context.write(key, newCenter);

	}
}