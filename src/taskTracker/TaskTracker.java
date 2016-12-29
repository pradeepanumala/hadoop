package taskTracker;

import java.rmi.RemoteException;


public class TaskTracker{
	static String  configFilesPath="/home/pradeep/hdfs/configFiles/";
	private TaskTrackerUtility util_ = null;
	
	public TaskTracker() throws RemoteException{
		super();
		util_ = new TaskTrackerUtility();
	}
	
	 private void initialize(){
		 util_.initialize();
	 }
	
	public static void main(String []args) throws RemoteException{
		if(args.length==1)
			  configFilesPath = args[0];
		TaskTracker tracker = new TaskTracker();
		tracker.initialize();
	}
}