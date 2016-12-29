package utility;

import java.util.List;

public class TaskTrackerHBInfo {
	
	private int taskTrackerId;
	private int freeReduceSlots;
	private List<MapTaskStatusMem> mStatus = null;
	private List<ReduceTaskStatusMem> rStatus = null;
	
	public int getTaskTrackerId() {
		return taskTrackerId;
	}
	private int freeMapSlots;
	public int getFreeMapSlots() {
		return freeMapSlots;
	}
	public int getFreeReduceSlots() {
		return freeReduceSlots;
	}
	public List<MapTaskStatusMem> getmStatus() {
		return mStatus;
	}
	public List<ReduceTaskStatusMem> getrStatus() {
		return rStatus;
	}
	
	public TaskTrackerHBInfo(int taskTrackerId, int freeMapSlots, int freeReduceSlots, List<MapTaskStatusMem> mStatus,
			List<ReduceTaskStatusMem> rStatus) {
		super();
		this.taskTrackerId = taskTrackerId;
		this.freeMapSlots = freeMapSlots;
		this.freeReduceSlots = freeReduceSlots;
		this.mStatus = mStatus;
		this.rStatus = rStatus;
	}
}

