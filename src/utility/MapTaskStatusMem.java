package utility;

public class MapTaskStatusMem{
	
	  int jobId;
	  int taskId;
	  private boolean taskCompleted = false;
	  private String mapOutputFile = null;
	  public boolean sentForReduce = false;
	  
	  public MapTaskStatusMem(int jobId, int taskId, boolean taskCompleted, String mapOutputFile) {
		super();
		this.jobId = jobId;
		this.taskId = taskId;
		this.taskCompleted = taskCompleted;
		this.mapOutputFile = mapOutputFile;
	}

	public int getJobId() {
		return jobId;
	}
	@Override
	public String toString(){
		return "MapTaskStatusMem object is:"+getJobId()+":"+getTaskId()+":"+this.mapOutputFile+":"+this.sentForReduce;
	}

	public int getTaskId() {
		return taskId;
	}


	public boolean isTaskCompleted() {
		return taskCompleted;
	}


	public String getMapOutputFile() {
		return mapOutputFile;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapOutputFile == null) ? 0 : mapOutputFile.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MapTaskStatusMem other = (MapTaskStatusMem) obj;
		if (mapOutputFile == null) {
			if (other.mapOutputFile != null)
				return false;
		} else if (!mapOutputFile.equals(other.mapOutputFile))
			return false;
		return true;
	}
}


