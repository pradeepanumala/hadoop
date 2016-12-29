package taskTracker;

import java.util.List;

public class ReduceTaskStatusData {
 private int jobId = -1 ;
 private int taskId = -1 ;
 private boolean taskCompleted = false ;
 List<String> fileList = null;
 
    public List<String> getFileList() {
	return fileList;
}
	public ReduceTaskStatusData(int jobId, int taskId, List<String>  fileList){
    	this.jobId = jobId;
    	this.taskId = taskId;
    	this.fileList = fileList;
    }
	   public int getJobId() {
		return jobId;
	}
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public boolean isTaskCompleted() {
		return taskCompleted;
	}
	public void setTaskCompleted(boolean taskCompleted) {
		this.taskCompleted = taskCompleted;
	}
	@Override
	public String toString(){
		return "ReduceTaskStatusData is:"+getJobId()+":"+getTaskId();
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + taskId;
		result = prime * result + jobId;
		return result;
	}
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReduceTaskStatusData other = (ReduceTaskStatusData) obj;
		if (jobId != other.jobId)
			return false;
		if (taskId != other.taskId)
			return false;
		return true;
	}
	
}
