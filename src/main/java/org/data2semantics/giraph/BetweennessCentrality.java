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
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Betweenness Centrality based on SimpleShortestPathComputation by Apache Boris
 * Mulder
 */
@Algorithm(name = "Shortest paths", description = "Finds all shortest paths from a selected vertex")
public class BetweennessCentrality extends BasicComputation<LongWritable, BCNode, FloatWritable, BCMessage> {
	/** The shortest paths id */
	public static final LongConfOption SOURCE_ID = new LongConfOption("SimpleShortestPathsVertex.sourceId", 1);
	/** Class logger */
	private static final Logger LOG = Logger.getLogger(BetweennessCentrality.class);

	
	/**
	 * Is this vertex the source id?
	 * 
	 * @param vertex
	 *            Vertex
	 * @return True if the source id
	 */
	private boolean isSource(Vertex<LongWritable, BCNode, FloatWritable> vertex) {
		return vertex.getId().get() == SOURCE_ID.get(getConf());
	}

	@Override
	public void compute(Vertex<LongWritable, BCNode, FloatWritable> vertex, Iterable<BCMessage> messages)
			throws IOException {
//		if (getSuperstep() == 0) {
//			vertex.setValue(new BCNode());
//		}
		
		double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
		long parentID = -1;

		// check all messages if there is a shorter path from the root. If there
		// is, get a new parent/distance.
		for (BCMessage message : messages) {
			if (message.getValue() < minDist) {
				minDist = message.getValue();
				parentID = message.getPred();
			}
		}

//		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist + " vertex value = "
					+ vertex.getValue().getDistance());
//		}

		if (minDist < vertex.getValue().getDistance()) {
			BCNode vertexValue = vertex.getValue().clone();
			vertexValue.setDistance(minDist);
			vertexValue.setParent(parentID);
			vertex.setValue(vertexValue);
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				double distance = minDist++;
//				if (LOG.isDebugEnabled()) {
					LOG.debug("Vertex " + vertex.getId().get() + " sent to " + edge.getTargetVertexId() + " = "
							+ distance);
//				}
				BCMessage msg = new BCMessage(vertex.getId().get(), distance);
				sendMessage(edge.getTargetVertexId(), msg);
			}
		}
		if (getSuperstep() <= 50 ) {
			vertex.voteToHalt();
		}
	}
}
