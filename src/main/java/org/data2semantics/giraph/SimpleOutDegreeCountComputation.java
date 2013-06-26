/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.data2semantics.giraph;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;

/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(name = "Outdegree Count")
public class SimpleOutDegreeCountComputation extends
		BasicComputation<Text, LongWritable, NullWritable, DoubleWritable> {

	@Override
	public void compute(Vertex<Text, LongWritable, NullWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		if (getSuperstep() == 0) {
			LongWritable vertexValue = vertex.getValue();
			vertexValue.set(vertex.getNumEdges());
			vertex.setValue(vertexValue);
			
			
			//we want to send a message to our target vertices as well. otherwise, our output only contains the vertices with an outdegree > 0
			Iterable<Edge<Text, NullWritable>> edges = vertex.getEdges();
			for (Edge<Text, NullWritable> edge : edges) {
				sendMessage(edge.getTargetVertexId(), new DoubleWritable(1.0));
			}
		} else {
			if (vertex.getNumEdges() == 0) {
				LongWritable vertexValue = vertex.getValue();
				vertexValue.set(0);
				vertex.setValue(vertexValue);
			} else {
				//if it has a number of outgoing edges, the proper value is already set in the first step
			}
			vertex.voteToHalt();
		}
	}
}
