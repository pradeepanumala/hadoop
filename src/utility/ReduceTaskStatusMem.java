package utility;

import java.util.List;

import taskTracker.MapTaskStatusData;

public class ReduceTaskStatusMem{
	
	
	  int jobId;
	  int taskId;
	  boolean taskCompleted = false;
	  List<String> fileList = null;
	public ReduceTaskStatusMem(int jobId, int taskId, boolean taskCompleted) {
		super();
		this.jobId = jobId;
		this.taskId = taskId;
		this.taskCompleted = taskCompleted;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + jobId;
		result = prime * result + taskId;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		MapTaskStatusMem other = (MapTaskStatusMem) obj;
		if (jobId != other.jobId)
			return false;
		if (taskId != other.taskId)
			return false;
		return true;
	}
	
	@Override
	public String toString(){
		return "Reduce Task Status:"+getJobId()+":"+getTaskId()+":"+isTaskCompleted()+":"+fileList;
	}
	public int getJobId() {
		return jobId;
	}
	public int getTaskId() {
		return taskId;
	}
	public boolean isTaskCompleted() {
		return taskCompleted;
	}
}
