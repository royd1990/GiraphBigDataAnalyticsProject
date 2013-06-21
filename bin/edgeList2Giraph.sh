#!/bin/bash
#first some default values
defaultNodeWeight=0
defaultEdgeWeight=1




nodeString=""
nodeId=""
function findNodeId {
	nodeLine=`grep -P "$nodeString" $dictFile`;
	if [ -z "$nodeLine" ]; then
		nodeId=""
	else
		nodeId=`echo "$nodeLine" | awk '{print $2}'`;
	fi
}
function addNodeToDict {
	echo "$1	$nodeIterator" >> $dictFile;
	nodeId=$nodeIterator
	nodeIterator=$(($nodeIterator + 1))
}





if [ -z "$1" ];then
	echo "at least 1 argument required (edgelist input file, in tab separated format)"
	echo "optional second arg for output file, and optional third arg for dictionary"
	exit
fi



inputFile="$1"
outputFile=`basename $inputFile`
outputFile+="_giraph"
if [ -n "$2" ];then
	outputFile="$2"
fi
dictFile=`basename $inputFile`
dictFile+="_dict"
if [ -n "$3" ];then
	dictFile="$3"
fi

sortedFile=`basename $inputFile`
sortedFile+="_sorted"

#init empty files (overwrites as well!)
> "$dictFile"
> "$outputFile"


echo "converting edgelist $inputFile into $outputFile"

prevNode1String=""
prevNode1Id=""
lineNum=0;
nodeIterator=0; #used for assigning IDs to nodes
outgoingEdges=()
sort $inputFile > "$sortedFile"
while read line; do
	lineNum=$(($lineNum + 1))
	node1=`echo "$line" | awk '{print $1}'`;
	node2=`echo "$line" | awk '{print $3}'`;
	if [ -z "$node1" ] || [ -z "$node2" ] ; then
		echo "WARN: unable to parse '$line' on line $lineNum. Skipping" 
		continue
	fi
	if [ "$prevNode1String" == "$node1" ]; then
		#find nodeId for outgoing edge
		nodeString="$node2"
		findNodeId;
		if [ -z "$nodeId" ] ; then
			addNodeToDict "$node2"
		fi
		outgoingEdges+=("$nodeId")
	else 
		if [ -n "$prevNode1String" ]; then
			#first store the previous results (if there are any. on first run there arent)
			edgesString="["
			for i in "${outgoingEdges[@]}"; do
			   edgesString+="[${i},${defaultEdgeWeight}],"
			done
			#need to remove final ','
			edgesString="${edgesString:0:${#edgesString}-1}]"
			echo "[${prevNode1Id},${defaultNodeWeight},${edgesString}]" >> "$outputFile"
		fi
			#[0,0,[[1,1],[3,3]]]
		
		#now reset values
		prevNode1String="$node1"
		
		nodeString="$node1"
		findNodeId;
		if [ -z "$nodeId" ] ; then
			addNodeToDict "$prevNode1String"
		fi
		prevNode1Id="$nodeId"
		
		outgoingEdges=()
		nodeString="$node2"
		findNodeId;
		if [ -z "$nodeId" ] ; then
			addNodeToDict "$node2"
		fi
		outgoingEdges+=("$nodeId")
	fi
done < "$sortedFile"
if [ -n "$prevNode1String" ]; then
	#store last iteration
	edgesString="["
	for i in "${outgoingEdges[@]}"; do
	   edgesString+="[${i},${defaultEdgeWeight}],"
	done
	#need to remove final ','
	edgesString="${edgesString:0:${#edgesString}-1}]"
	echo "[${prevNode1Id},${defaultNodeWeight},${edgesString}]" >> "$outputFile"
		#[0,0,[[1,1],[3,3]]]
fi
