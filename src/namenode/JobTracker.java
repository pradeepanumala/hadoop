package namenode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.InvalidProtocolBufferException;

import protobuf.MapReduce.*;
import utility.MapTaskStatusMem;
import utility.ReduceTaskStatusMem;
import utility.TaskTrackerHBInfo;
;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected JobTracker() throws RemoteException {
		super();
		tasktrackerHBInfo = new HashMap<Integer, TaskTrackerHBInfo>();
		ttfreeSlotCount = new HashMap<Integer, Integer>();
		
	}
	
	JobTrackerUtility jtutility = new JobTrackerUtility();
	private Map<Integer,TaskTrackerHBInfo> tasktrackerHBInfo = null;
	private Map<Integer,Integer> ttfreeSlotCount = null;
	//JobQueue maintains the queue of jobs submitted by client
	Queue<JobData> jobQueue = new LinkedList<JobData>();
	// JobProgress is the data class to track the status/progress of a job
	static HashMap<Integer,JobProgress> jobProgress = new HashMap<Integer,JobProgress>();
	static Map<Integer,Set<DataNode>> JobBlockData = new HashMap<Integer,Set<DataNode>>();
	static ConcurrentMap<Integer,Set<MapTaskStatusMem>> jobMapStatus = new ConcurrentHashMap<Integer,Set<MapTaskStatusMem>>();
	static ConcurrentMap<Integer,Set<ReduceTaskStatusMem>> jobReduceStatus = new ConcurrentHashMap<Integer,Set<ReduceTaskStatusMem>>();
	ConcurrentMap<Integer,Integer> jobReduceCount = new ConcurrentHashMap<Integer,Integer>();
	static JobData CurrentJobData =null;
	static String configFilesPath="/home/pradeep/hdfs/configFiles/";
	static String nameNodeIp=null;
	static int nameNodePort;
    static AtomicInteger JobIdSeq=new AtomicInteger(0);
    static AtomicInteger TaskIdSeq=new AtomicInteger(0);
    static AtomicInteger reduceCount= new AtomicInteger(0);
    Set<String> reducerSubmittedFiles = new HashSet<String>();
    ConcurrentMap<Integer,Set<String>> reduceTaskFiles = new ConcurrentHashMap<Integer,Set<String>>();
     
    
  //  static int ReduceTaskIdSeq;
    static int CurrentJob=0;
    static int NumReduceTasks;
    boolean isJobInProgress=false, isJobCompleted = false;;
    
	public static void main(String args[]) throws RemoteException{
		JobTracker jTracker = new JobTracker();
		if(args.length==1)
			  configFilesPath = args[0];
		
		String sCurrentLine;
		BufferedReader br = null;
		try{
		br = new BufferedReader(new FileReader(configFilesPath+"namenodeip.txt"));
		while ((sCurrentLine = br.readLine()) != null) {
			nameNodeIp=sCurrentLine.split(":")[0];
			nameNodePort=Integer.parseInt(sCurrentLine.split(":")[1].trim());
			break;
		}
			Naming.rebind("rmi://"+nameNodeIp+":"+nameNodePort+"/jTracker", jTracker);
			System.out.println("Job Tracker Started");
		} catch (Exception e) {

			e.printStackTrace();
		}
	}


	@Override
	public byte[] jobSubmit(byte[] inp){
		 
		JobSubmitRequest jsr=null;
		String MapName =null, ReducerName=null, InputFile=null, OutputFile= null;
		//int NumReduceTasks = 0;
		try {
			jsr = JobSubmitRequest.parseFrom(inp);
			MapName = jsr.getMapName();
			ReducerName = jsr.getReducerName();
			InputFile = jsr.getInputFile();
			OutputFile = jsr.getOutputFile();
			NumReduceTasks=jsr.getNumReduceTasks();
			
			ArrayList<Integer> BlockList=JobTrackerUtility.filenblocks.get(InputFile);
			JobData jobData = new JobData(JobIdSeq.incrementAndGet(),MapName, ReducerName, InputFile, OutputFile, NumReduceTasks,BlockList);
			jobReduceCount.put(JobIdSeq.get(), NumReduceTasks);
			jobQueue.add(jobData);
			if(!isJobInProgress){ 
				CurrentJobData=jobQueue.remove();
				reduceCount.set(CurrentJobData.getNumReduceTasks());
	    		jtutility.readDataNodeInfo(configFilesPath+"blockData.txt");
				for(Integer block:BlockList){
					JobBlockData.put(block, jtutility.dnMap.get(block));
				}
				jobProgress.put(JobIdSeq.get(),new JobProgress(JobBlockData,MapName,ReducerName));
				isJobInProgress=true;
				CurrentJob=JobIdSeq.get();
			}
			JobSubmitResponse.Builder jsRespStream = JobSubmitResponse.newBuilder();
			jsRespStream.setJobId(JobIdSeq.get());
			jsRespStream.setStatus(1);
			
			return jsRespStream.build().toByteArray();

		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public synchronized void updateJobProgress(List<MapTaskStatusMem> mMem,List<ReduceTaskStatusMem> rMem){
		if(mMem.size()>0){
	//		System.out.println("mMem is not null");
		jobProgress.get(mMem.get(0).getJobId()).updateMapTaskStatusList(mMem);
		Set<MapTaskStatusMem> sMem = new HashSet<MapTaskStatusMem>();
		for(MapTaskStatusMem m:mMem){
			if(m.getJobId() == CurrentJob)
				sMem.add(m);
		}
		if(jobMapStatus.containsKey(CurrentJob)){
			jobMapStatus.replace(CurrentJob, sMem);
		}else{
			jobMapStatus.put(CurrentJob, sMem);
		}
		Set<ReduceTaskStatusMem> rSMem = new HashSet<ReduceTaskStatusMem>();
		for(ReduceTaskStatusMem r:rSMem){
			if(r.getJobId() == CurrentJob)
				rSMem.add(r);
		}
		if(jobReduceStatus.containsKey(CurrentJob)){
			jobReduceStatus.replace(CurrentJob, rSMem);
		}else{
			jobReduceStatus.put(CurrentJob, rSMem);
		}
		
		}
		
		if(rMem.size()>0){
		jobProgress.get(rMem.get(0).getJobId()).updateReduceTaskStatusList(rMem);
		}
	}
	@Override
	public byte[] getJobStatus(byte[] inp){
		System.out.println("Inside job Status method");
		JobStatusResponse.Builder jb=null;
		try{
				
		
		JobStatusRequest jsrb = JobStatusRequest.parseFrom(inp);
		int jobId = jsrb.getJobId();
		int jobDone, totalMapTaskStarted=0, numMapTaskStarted= 0,totalReduceTasks, numReduceTaskStarted=0;
		boolean reduceTaskStatus=false;
		jb = JobStatusResponse.newBuilder();
		//while(!isJobCompleted){
			System.out.println("Job is in progress");
			if(jobMapStatus.containsKey(jobId)){
				numMapTaskStarted = jobMapStatus.get(jobId).size();
		
				for(ReduceTaskStatusMem rStats:jobReduceStatus.get(jobId)){
					if(!rStats.isTaskCompleted()){
						reduceTaskStatus=false;
						numReduceTaskStarted++;
				}
			}
			//}
			totalReduceTasks = 0;
			
			if(isJobCompleted)
				jb.setStatus(1);
			else
				jb.setStatus(0);
			
			jb.setJobDone(isJobCompleted);
			jb.setNumMapTasksStarted(numMapTaskStarted);
			jb.setNumReduceTasksStarted(numReduceTaskStarted);
			return jb.build().toByteArray();
			}
		//System.out.println("jb is being built "+jb);
		jb.setJobDone(reduceTaskStatus);
		jb.setNumMapTasksStarted(numMapTaskStarted);
		jb.setNumReduceTasksStarted(numReduceTaskStarted);
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
		// next is status to be handled
	
		return jb.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] inp){
		System.out.println("Recieved hearbeat request");
		int mJobId,NumMapSlotsFree,NumReduceSlotsFree,TaskTrackerId;
		List<Integer> toBeRemoved = new ArrayList<Integer>();
		HeartBeatResponse.Builder hbrespStream=null;
		Map<Integer, Set<DataNode>> pendingBlocks=null;
		if(isJobInProgress){
			System.out.println("There is a job in progress:"+CurrentJob);
			pendingBlocks=jobProgress.get(CurrentJob).pendingBlocks;
		}else
			System.out.println("There is no job in progress ");
		try {
			
			HeartBeatRequest heartBeatRequest = HeartBeatRequest.parseFrom(inp);
			
			 NumMapSlotsFree=heartBeatRequest.getNumMapSlotsFree();
			 NumReduceSlotsFree=heartBeatRequest.getNumReduceSlotsFree();
			 TaskTrackerId=heartBeatRequest.getTaskTrackerId();
			 
			List<MapTaskStatus> mstatus = heartBeatRequest.getMapStatusList();
			List<ReduceTaskStatus> rstatus = heartBeatRequest.getReduceStatusList();
			List<MapTaskStatusMem> mMem = new LinkedList<MapTaskStatusMem>();
			List<ReduceTaskStatusMem> rMem = new LinkedList<ReduceTaskStatusMem>();
			
			for(MapTaskStatus mst: mstatus){
				MapTaskStatusMem m = new MapTaskStatusMem(mst.getJobId(), mst.getTaskId(),mst.getTaskCompleted(),mst.getMapOutputFile());
				mMem.add(m);
			}
			for(ReduceTaskStatus rst: rstatus){
				
				ReduceTaskStatusMem r = new ReduceTaskStatusMem(rst.getJobId(), rst.getTaskId(), rst.getTaskCompleted());
			
				if(r.isTaskCompleted()){
					reduceTaskFiles.remove(r.getTaskId());
					if(reduceTaskFiles.size()==0){
						isJobInProgress = false;
						isJobCompleted = true;
						jobProgress.remove(CurrentJobData);
						System.out.println("All the Reducer Tasks of :"+CurrentJob + " are completed");
					}
				}else{
					rMem.add(r);
				}
			}
			if(isJobInProgress)
				updateJobProgress(mMem,rMem);
			System.out.println("NumMapSlotsFree:"+NumMapSlotsFree+",NumReduceSlotsFree:"+NumReduceSlotsFree);
			if(jobReduceCount.containsKey(CurrentJob))
				reduceCount.set(jobReduceCount.get(CurrentJob));
			hbrespStream = HeartBeatResponse.newBuilder();
			if(NumReduceSlotsFree>0 && isJobInProgress && (pendingBlocks == null || pendingBlocks.isEmpty()) ){
				  System.out.println("All the map jobs are submitted..Checking if map jobs are done.....");
				  if(jtutility.areAllMapTasksCompleted(CurrentJob)){
					  System.out.println("All map jobs are completed");
					  
					  while(NumReduceSlotsFree>0 && reduceCount.get()>0){ 
						  
						  List<String> mapOutputFileNames  = jtutility.getMapOutputFileNames();
						  for(String newFile:mapOutputFileNames){
							  if(reducerSubmittedFiles.contains(newFile)){
								  continue;
							  }
							  System.out.println("MapOutputFIleNames:"+mapOutputFileNames);
							  reducerSubmittedFiles.addAll(mapOutputFileNames);
							  System.out.println("The following files are assinged to the reducer with task id "+TaskIdSeq.get());
							  
							  ReducerTaskInfo.Builder rtiStream = ReducerTaskInfo.newBuilder();
							  rtiStream.setJobId(CurrentJob);
							  rtiStream.setTaskId(TaskIdSeq.incrementAndGet());
							  rtiStream.setReducerName(jobProgress.get(CurrentJob).Reducer);
							  rtiStream.addAllMapOutputFiles(mapOutputFileNames);
							  System.out.println("Adding files for reduce:"+mapOutputFileNames);
							  NumReduceSlotsFree --;
							  reduceCount.decrementAndGet();
							  //System.out.println("Num free slow becomes:"+NumReduceSlotsFree+" & reduceCount is:"+reduceCount.get());
							  hbrespStream.addReduceTasks(rtiStream);
							  Set<String> sentFiles = new HashSet<String>();
							  sentFiles.addAll(mapOutputFileNames);
							  reduceTaskFiles.put(TaskIdSeq.get(), sentFiles);
						  }
					  }
				  }
			  }else if(NumMapSlotsFree>0 && isJobInProgress){
		    		
				for (Map.Entry<Integer, Set<DataNode>> entry : pendingBlocks.entrySet())
				{  
					//System.out.println("The no of free map slots is "+NumMapSlotsFree);
					if(NumMapSlotsFree==0){break;}
					MapTaskInfo.Builder mtiStream = MapTaskInfo.newBuilder();
		    		MapTaskStatusMem mtStatus = new MapTaskStatusMem(CurrentJob, TaskIdSeq.incrementAndGet(),false,null);
		    		jobProgress.get(CurrentJob).mapTaskStatusList.add(mtStatus);
					//System.out.println("The pending block is added to jobprogress");
					BlockLocations.Builder blStream = BlockLocations.newBuilder();
		    		blStream.setBlockNumber(entry.getKey());
				    for(DataNode d: entry.getValue()){
				    	DataNodeLocation.Builder dnlStream = DataNodeLocation.newBuilder();
				    		dnlStream.setIp(d.getIp());
				    		dnlStream.setPort(d.getPort());
				    		blStream.addLocations(dnlStream);
				    		
				    }
				    		mtiStream.setJobId(CurrentJob);
				    		mtiStream.setTaskId(TaskIdSeq.get());				    		
				    		mtiStream.setMapName(CurrentJobData.getMapName());
				    		mtiStream.addInputBlocks(blStream);
				    		
				    		toBeRemoved.add(entry.getKey());
				    		NumMapSlotsFree--;
				    		hbrespStream.addMapTasks(mtiStream);
				}
				for(Integer block:toBeRemoved){
		    		pendingBlocks.remove(block);
				}
			  }
			
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		//System.out.println("Before sending heartBeat");
		System.out.println("\n\n");
		return hbrespStream.build().toByteArray();
	}
	
	
}