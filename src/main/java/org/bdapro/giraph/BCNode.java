package org.data2semantics.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class BCNode implements Writable {

	private long parent;
	private double distance;
	private int betweennessCentrality;
	
	public BCNode(){
		parent = -1;
		distance = Integer.MAX_VALUE;
		betweennessCentrality = 0;
	}
	
	public BCNode(long p, double d, int bc){
		parent = p;
		distance = d;
		betweennessCentrality = bc;
	}
	
	public double getDistance(){
		return distance;
	}
	
	public long getParentID(){
		return parent;
	}
	
	public int getBC(){
		return betweennessCentrality;
	}
	
	public void setDistance(double d){
		distance = d;
	}
	
	public void setParent(long p){
		parent = p;
	}
	
	public void setBC(int bc){
		betweennessCentrality = bc;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		parent = arg0.readLong();
		distance = arg0.readDouble();
		betweennessCentrality = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(parent);
		arg0.writeDouble(distance);
		arg0.writeInt(betweennessCentrality);
	}
	
	public String toString(){
		return "parent="+ parent + ", dist=" + distance + ", bc=" + 
				betweennessCentrality;
	}
	
	public BCNode clone(){
		return new BCNode(parent, distance, betweennessCentrality);
	}
}
