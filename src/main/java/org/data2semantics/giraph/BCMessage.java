package org.data2semantics.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BCMessage implements Writable {

	private long pred;
	private double value;
	
	BCMessage(long pred, double value){
		this.pred=pred;
		this.value=value;
	}
	
	public long getPred(){
		return pred;
	}
	
	public double getValue(){
		return value;
	}
	
	public void setPred(long id){
		pred = id;
	}
	
	public void setValue(double dist){
		value = dist;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		pred = arg0.readLong();
		value = arg0.readDouble();
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(pred);
        arg0.writeDouble(value);
	}
	
	public String toString(){
		return "pred="+ pred + ", dist=" + value;
	}
	
	public BCMessage clone(){
		return new BCMessage(pred, value);
	}
}
