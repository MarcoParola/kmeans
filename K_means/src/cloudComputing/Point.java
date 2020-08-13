package cloudComputing;

import java.util.ArrayList;

public class Point {
	private ArrayList<Float> attributeValues = new ArrayList<Float>();
	
	public Point(ArrayList<Float> attr) {
		for(int i = 0; i < attr.size(); i++) {
			this.attributeValues.add(attr.get(i));
		}
	}
	
	// constructor through String
	public Point(String str) {
		String[] attr = str.split(",");
		for(int i = 0; i < attr.length; i++) {
			Float value = Float.parseFloat(attr[i]);
			this.attributeValues.add(value);
		}
	}
	
	public ArrayList<Float> getAttributeValues(){
		return attributeValues;
	}
	
	public void setAttributeValues(ArrayList<Float> value) {
		this.attributeValues = value;
	}
	
	

	public double computeDistance(Point points) {
		double ret = 0.0f;
		for(int i = 0; i < this.attributeValues.size(); i++) {
			double value = this.attributeValues.get(i) - points.getAttributeValues().get(i);
			ret += value * value;
			
		}
		ret = Math.sqrt(ret);
		
		return ret;
	}
}
