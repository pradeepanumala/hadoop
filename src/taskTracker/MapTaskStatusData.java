package taskTracker;

public class MapTaskStatusData {
	int jobId =-1;
   int taskId =-1;
   boolean taskCompleted = false;
   String mapOutputFile = null;
   
   public MapTaskStatusData(){
	   
   }
   public MapTaskStatusData(int jobId, int taskId){
	   this.jobId = jobId;
	   this.taskId = taskId;
   }
   
   public MapTaskStatusData(int jobId, int taskId, String mapOutputFile){
	   this.jobId = jobId;
	   this.taskId = taskId;
	   this.mapOutputFile = mapOutputFile;
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
	public String getMapOutputFile() {
		return mapOutputFile;
	}
	public void setMapOutputFile(String mapOutputFile) {
		this.mapOutputFile = mapOutputFile;
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
		MapTaskStatusData other = (MapTaskStatusData) obj;
		if (jobId != other.jobId)
			return false;
		if (taskId != other.taskId)
			return false;
		return true;
	}
}
