package namenode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

import utility.MapTaskStatusMem;

public class JobTrackerUtility {
    static HashMap<String,ArrayList<Integer>> filenblocks = new HashMap<String,ArrayList<Integer>>();
    Map<Integer, Set<DataNode>> dnMap = null;
    public JobTrackerUtility(){
    	loadfilenblocks();
    	readDataNodeInfo(JobTracker.configFilesPath+"blockData.txt");
    	//System.out.println("dnmap is"+dnMap);
    }
	public void loadfilenblocks(){
    BufferedReader br = null;
	try {

		String sCurrentLine;
		br = new BufferedReader(new FileReader(JobTracker.configFilesPath+"nnconfig.txt"));
		
		while ((sCurrentLine = br.readLine()) != null) {
			System.out.println(sCurrentLine);
			String[] splitstr = sCurrentLine.split(":");
			String[] blocks = splitstr[1].split(",");

			ArrayList<Integer> blocklist = new ArrayList<Integer>();
	
		    for(String blockNum:blocks){
		    	blocklist.add(Integer.parseInt(blockNum.trim()));
		    }
			filenblocks.put(splitstr[0].trim(), blocklist);
		}
	}catch(Exception e){
		e.printStackTrace();
	}
	
	}
	
	public INameNode connectToNameNode(String ipAddress, int portNo){
		INameNode nameNode = null;
		try {
			System.out.println("IpAddress is:"+ipAddress);
			nameNode= (INameNode)Naming.lookup("rmi://"+ipAddress+":"+portNo+"/nnserver");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			//e.printStackTrace();
			System.out.println("Not able to connect to NameNode");
			return null;
		}
		return nameNode;
	}
	

	public void removeCompletedMapTasks(int jobId, List<String> mapSubmittedFiles){
		JobProgress progress = JobTracker.jobProgress.get(jobId);
		for(String file:mapSubmittedFiles){
			MapTaskStatusMem mem= new MapTaskStatusMem(jobId, 0, false, file);
			progress.mapTaskStatusList.remove(mem);
			System.out.println("removing:"+mem);
		}
	}
	
	public boolean areAllMapTasksCompleted(int jobId){
		JobProgress progress = JobTracker.jobProgress.get(jobId);
		for(MapTaskStatusMem mem: progress.mapTaskStatusList){
			//System.out.println("Checking for mapTask Status:"+mem.toString());
				if(!mem.isTaskCompleted()){
					return false;
				}
			
		}
		return true;
	}
	
	public synchronized void readDataNodeInfo(String filePath){
		
		try{
			FileReader fr = new FileReader(new File(filePath));
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			dnMap = new HashMap<Integer, Set<DataNode>>();
			while((line=br.readLine())!=null){
				String []kv = line.split("-");
				int blockNumber = Integer.parseInt(kv[0]);
				String data[] = kv[1].split(",");
				DataNode node = null;
				Set<DataNode> dnS = new HashSet<DataNode>();
				for(String d:data){
					String dn[] = d.split(":");
					node = new DataNode(1,dn[0],Integer.parseInt(dn[1]));
					dnS.add(node);
				}
				dnMap.put(blockNumber, dnS);
			}
		//	System.out.println("after reading from file "+dnMap);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void prepareReduceRequest(){
		try{
			
		}catch(Exception ex){
			
		}
	}
	
	public List<String> getMapOutputFileNames(){
		List<String> mapOutputFileNames = new LinkedList<String>();
		 int totalMapTasks = JobTracker.jobProgress.get(JobTracker.CurrentJob).mapTaskStatusList.size();
		 int counter = 0;
		 int tasksPerReducer= totalMapTasks/JobTracker.NumReduceTasks;
		 //System.out.println("Before checking...");
		 for(MapTaskStatusMem mem: JobTracker.jobProgress.get(JobTracker.CurrentJob).mapTaskStatusList){
			 //System.out.println("MapStatus Mem is:"+mem.toString());
			 if(!mem.sentForReduce){
				 mem.sentForReduce=true;
				 mapOutputFileNames.add(mem.getMapOutputFile());
				 counter++;
			 }
			 if(counter == tasksPerReducer)
				 break;
		}
		 return mapOutputFileNames;
	}
	

	
}
