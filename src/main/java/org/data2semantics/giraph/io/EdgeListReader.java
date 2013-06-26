package org.data2semantics.giraph.io;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

public class EdgeListReader extends TextEdgeInputFormat<Text, NullWritable> {

	@Override
	public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new TextTextEdgeReader();
	}
	
	class TextTextEdgeReader extends TextEdgeReaderFromEachLine {
		private static final String SPLIT = "\\t";

		@Override
		protected Text getSourceVertexId(Text line) throws IOException {
			String[] splits = line.toString().split(SPLIT);
			if (splits.length < 2) {
				throw new IOException("unable to get source vertex Id from line '" + line + "'");
			}
			return new Text(splits[0]);
		}

		@Override
		protected Text getTargetVertexId(Text line) throws IOException {
			String[] splits = line.toString().split(SPLIT);
			if (splits.length < 2) {
				throw new IOException("unable to get target vertex Id from line '" + line + "'");
			}
			return new Text(splits[1]);
		}

		@Override
		protected NullWritable getValue(Text line) throws IOException {
			return NullWritable.get();
		}
	}
}
