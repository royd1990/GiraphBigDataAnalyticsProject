GiraphAnalysis
==============

##installation

Graph analysis using the Giraph apache incubator project

To compile:  
```mvn package```

Running the analysis requires a compiled trunk version of Giraph:  
```
git clone git@github.com:apache/giraph.git  
cd giraph  
mvn install
```

##Analysis
###pagerank
```
hadoop jar target/Analysis-0.0.1-SNAPSHOT-jar-with-dependencies.jar\  
org.apache.giraph.GiraphRunner  org.data2semantics.giraph.pagerank.PageRankComputation \  
-eif org.data2semantics.giraph.io.EdgeListReader  \  
-of org.apache.giraph.io.formats.IdWithValueTextOutputFormat \  
-mc org.data2semantics.giraph.pagerank.RandomWalkVertexMasterCompute \  
-wc org.data2semantics.giraph.pagerank.RandomWalkWorkerContext
-op <outputPath> \  
-eip <input edge list> \  
-w <number of workers>
```

This executed pagerank for inputfile ```<input edge list>```, writes output to directory ```<outputPath>```, and uses this amount of workers: ```<number of workers>```

###outdegree
```
hadoop jar target/Analysis-0.0.1-SNAPSHOT-jar-with-dependencies.jar\  
org.apache.giraph.GiraphRunner  org.data2semantics.giraph.pagerank.SimpleOutDegreeCountComputation \  
-eif org.data2semantics.giraph.io.EdgeListReader  \  
-of org.apache.giraph.io.formats.IdWithValueTextOutputFormat \  
-op <outputPath> \  
-eip <input edge list> \  
-w <number of workers>
```

This executed pagerank for inputfile ```<input edge list>```, writes output to directory ```<outputPath>```, and uses this amount of workers: ```<number of workers>```

###outdegree
```
hadoop jar target/Analysis-0.0.1-SNAPSHOT-jar-with-dependencies.jar\  
org.apache.giraph.GiraphRunner  org.data2semantics.giraph.pagerank.SimpleInDegreeCountComputation \  
-eif org.data2semantics.giraph.io.EdgeListReader  \  
-of org.apache.giraph.io.formats.IdWithValueTextOutputFormat \  
-op <outputPath> \  
-eip <input edge list> \  
-w <number of workers>
```

This executed pagerank for inputfile ```<input edge list>```, writes output to directory ```<outputPath>```, and uses this amount of workers: ```<number of workers>```
