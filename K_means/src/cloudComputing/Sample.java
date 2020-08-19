package cloudComputing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Sample implements Writable {
	
	int size;
	double[] attributeValues;

	public Sample() {}
	
	public Sample(int s, double[] attr) {
		size = s;
		
		attributeValues = new double[size];
		for(int i = 0; i < size; i++) {
			attributeValues[i] = attr[i]; 
		}
	}
	
	public Sample(int s) {
		size = s;
		attributeValues = new double[size];
		for(int i = 0; i < size; i++) {
			attributeValues[i] = 0; 
		}
	}
	
	public Sample(String str) {
		String[] attr = str.split(" ");
		size = attr.length;
		attributeValues = new double[size];
		for(int i = 0; i < attr.length; i++) {
			double value = Double.parseDouble(attr[i]);
			this.attributeValues[i] = value;
		}
	}
	
	public double[] getAttributeValues(){
		return attributeValues;
	}
	
	public void setAttributeValues(double[] values) {
		for(int i = 0; i < size; i++) 
			attributeValues[i] = values[i]; 
	}
	
	public int getSize(){
		return size;
	}
	
	public double computeDistance(Sample points) {
		double ret = 0.0f;
		for(int i = 0; i < points.getAttributeValues().length; i++) {
			double value = this.attributeValues[i]- points.getAttributeValues()[i];
			ret += value * value;
			
		}
		ret = Math.sqrt(ret);
		
		return ret;
	}


	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for(int i=0; i < size; i++)
			out.writeDouble(attributeValues[i]);
	}


	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		attributeValues = new double[size];
		for(int i=0; i < size; i++) 
			attributeValues[i] = in.readDouble();
	}
	
	@Override
	public String toString() {
		
		String ret = "";
		
		for(double val : attributeValues)
			ret += val + " ";
		
		return ret;
	}
}


