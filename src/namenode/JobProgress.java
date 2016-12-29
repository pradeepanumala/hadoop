package namenode;

import java.util.*;

import utility.MapTaskStatusMem;
import utility.ReduceTaskStatusMem;

public class JobProgress {
int jobid;
//taskId - Status
//HashMap<Integer,Boolean> taskStatus=null;
Map<Integer,Set<DataNode>>  pendingBlocks =new HashMap<Integer,Set<DataNode>>();
List<MapTaskStatusMem> mapTaskStatusList= new ArrayList<MapTaskStatusMem>();
List<ReduceTaskStatusMem> reduceTaskStatusMem= new ArrayList<ReduceTaskStatusMem>();
String Mapper,Reducer;
public JobProgress(Map<Integer,Set<DataNode>> pendingBlocks,String Mapper,String Reducer){
	this.pendingBlocks=pendingBlocks;
	this.Mapper=Mapper;
	this.Reducer=Reducer;
}


public void updateMapTaskStatusList(List<MapTaskStatusMem> mapTaskStatusList){
	this.mapTaskStatusList=mapTaskStatusList;
}

public void updateReduceTaskStatusList(List<ReduceTaskStatusMem> reduceTaskStatusMem){
	this.reduceTaskStatusMem.clear();
	this.reduceTaskStatusMem=reduceTaskStatusMem;
}


}
