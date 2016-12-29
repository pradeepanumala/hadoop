package namenode;
import java.util.*;
public class JobData {
private String MapName =null, ReducerName=null, InputFile=null, OutputFile= null;
private int NumReduceTasks,JobId;
private ArrayList<Integer> BlockList=null;
public JobData(int jobId,String mapName, String reducerName, String inputFile, String outputFile, int numReduceTasks,ArrayList<Integer> BlockList) {
	super();
	MapName = mapName;
	ReducerName = reducerName;
	InputFile = inputFile;
	OutputFile = outputFile;
	NumReduceTasks = numReduceTasks;
	JobId=jobId;
}

public String getMapName() {
	return MapName;
}

public String getReducerName() {
	return ReducerName;
}
public String getInputFile() {
	return InputFile;
}
public String getOutputFile() {
	return OutputFile;
}
public int getNumReduceTasks() {
	return NumReduceTasks;
}
public int getJobId() {
	return JobId;
}
public ArrayList<Integer> getBlockList() {
	return BlockList;
}
}
