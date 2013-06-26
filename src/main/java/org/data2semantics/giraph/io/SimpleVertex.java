package org.data2semantics.giraph.io;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SimpleVertex extends TextVertexValueInputFormat<Text, NullWritable, NullWritable> {
	@Override
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
