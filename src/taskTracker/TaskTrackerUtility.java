	package taskTracker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;

import datanode.IDataNode;
import namenode.IJobTracker;
import protobuf.Hdfs.ReadBlockRequest;
import protobuf.Hdfs.ReadBlockResponse;
import protobuf.MapReduce.BlockLocations;
import protobuf.MapReduce.DataNodeLocation;
import protobuf.MapReduce.HeartBeatRequest;
import protobuf.MapReduce.HeartBeatResponse;
import protobuf.MapReduce.MapTaskInfo;
import protobuf.MapReduce.MapTaskStatus;
import protobuf.MapReduce.ReduceTaskStatus;
import protobuf.MapReduce.ReducerTaskInfo;
import utility.ReduceTaskStatusMem;

public class TaskTrackerUtility {
	
	//private MasterWorker mWorker = null;
	private MasterTaskWorker masWorker = null;
    int noOfMFreeWorkers = MasterTaskWorker.MAX_THREADS;
    int noOfRFreeWorkers = MasterTaskWorker.MAX_THREADS;
	static String myIp=null;
	static int myPort,nameNodePort,jobTrackerPort;
	static int blockSize=100;
	public static String nameNodeIp,jobTrackerIp,blocksDataPath,mapOutputPath,intermediateFolder ,reducerOutputFolder,grepWord = null;
			
	List<MapTaskStatusData> mapTaskStatusList = new ArrayList<MapTaskStatusData>();
	static ConcurrentMap<MapTaskStatusData,Boolean> mTStatusMap = new ConcurrentHashMap<MapTaskStatusData,Boolean>();
	static ConcurrentMap<ReduceTaskStatusData,Boolean> rTStatusMap = new ConcurrentHashMap<ReduceTaskStatusData,Boolean>();
	
	List<ReduceTaskStatusData> reduceTaskStatusList = new ArrayList<ReduceTaskStatusData>();
	boolean debug;
	
	private byte taskTrackerId;// to be loaded from configuration file
	public TaskTrackerUtility(){
		masWorker = new MasterTaskWorker(this);
	}
	public void initialize(){
		loadConfigurations();
		sendHearBeatToJobTracker();
		System.out.println("Task Tracker Started");
	}
	
	
	private void loadConfigurations() {
		
		try { @SuppressWarnings("resource")
		String line;
		BufferedReader br = new BufferedReader(new FileReader(TaskTracker.configFilesPath+"ttconfig.txt"));
			while ((line = br.readLine()) != null) {
				String key=line.split(":")[0];
				String value=line.split(":")[1];
				if(key.equalsIgnoreCase("ip")){
					myIp=value;
				}
				else if (key.equalsIgnoreCase("port")){
					myPort=Integer.parseInt(value);
				}
				else if (key.equalsIgnoreCase("jobTrackerIp")){
					jobTrackerIp=value;
				}
				else if (key.equalsIgnoreCase("jobTrackerPort")){
					jobTrackerPort=Integer.parseInt(value);
				}				
				else if (key.equalsIgnoreCase("blocksDataPath")){
					blocksDataPath=value;
				}
				else if (key.equalsIgnoreCase("mapOutputPath")){
					mapOutputPath=value;
				}
				else if (key.equalsIgnoreCase("intermediateFolder")){
					intermediateFolder=value;
				}
				else if (key.equalsIgnoreCase("reducerOutputFolder")){
					reducerOutputFolder=value;
				}	
				else if (key.equalsIgnoreCase("nameNodeIp")){
					nameNodeIp=value;
				}	
				else if (key.equalsIgnoreCase("nameNodePort")){
					nameNodePort=Integer.parseInt(value);
				}	
				else if (key.equalsIgnoreCase("grepWord")){
					grepWord=value;
				}else if (key.equalsIgnoreCase("blockSize")){
					blockSize=Integer.parseInt(value);
				}				
			}
			printToScreen("The ip loaded for tasktracker is "+myIp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void sendHearBeatToJobTracker(){
			System.out.println("HearBeat Started\n");
			Runnable runnable = new Runnable() {
			      public void run() {
			    	  byte[] heartBeatResp = null;
			        // task to run goes here
			    	    System.out.println("Sending heartbeat to JobTracker");
			    		HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
			    		heartBeatRequest.setTaskTrackerId(1);
			    		/*noOfMFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getMapActiveCount();
			    		noOfRFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getReduceActiveCount();*/
			    		/*heartBeatRequest.setNumMapSlotsFree(noOfMFreeWorkers);
			    		heartBeatRequest.setNumReduceSlotsFree(noOfRFreeWorkers);*/
			    		
			    		MapTaskStatus.Builder mts= MapTaskStatus.newBuilder();
			    		for(MapTaskStatusData m: mTStatusMap.keySet()){
			    		   mts.setJobId(m.getJobId());
			    		   mts.setTaskId(m.getTaskId());
			    		   mts.setTaskCompleted(mTStatusMap.get(m));
			    		   mts.setMapOutputFile(m.getMapOutputFile());
				    	   heartBeatRequest.addMapStatus(mts);
			    		}
			    		ReduceTaskStatus.Builder rts= ReduceTaskStatus.newBuilder();
			    		//System.out.println("ReduceTask List size is:"+rTStatusMap.size());
			    		Set<ReduceTaskStatusData> listTobeRemoved = new HashSet<ReduceTaskStatusData>();
			    		for(ReduceTaskStatusData r: rTStatusMap.keySet()){
				    		   rts.setJobId(r.getJobId());
				    		   rts.setTaskId(r.getTaskId());
				    		   rts.setTaskCompleted(rTStatusMap.get(r));
				    		   //System.out.println("Reduce task status is:"+rTStatusMap.get(r)+" for:"+r.getFileList());
				    		   if(rts.getTaskCompleted()){
				    			   listTobeRemoved.add(r);
				    			   System.out.println("Reduce task:"+rts.getTaskId()+" is completed");
				    		   }
					    	   heartBeatRequest.addReduceStatus(rts);
				    	}
			    		for(ReduceTaskStatusData r:listTobeRemoved){
			    			rTStatusMap.remove(r);
			    		}
			    		//System.out.println("HeartBeat Request Size:"+heartBeatRequest.getReduceStatusCount()+":"+heartBeatRequest.getMapStatusCount());
			    		noOfMFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getMapActiveCount();
			    		noOfRFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getReduceActiveCount();
			    		System.out.println("Free Mapper Workers:"+noOfMFreeWorkers+",Free Reducer Workers:"+noOfRFreeWorkers);
			    		heartBeatRequest.setNumMapSlotsFree(noOfMFreeWorkers);
			    		heartBeatRequest.setNumReduceSlotsFree(noOfRFreeWorkers);
			    		byte []heartBeatByte = heartBeatRequest.build().toByteArray();
			    		IJobTracker jTracker = establishConnectionWithJobTracker(jobTrackerIp,jobTrackerPort);
			    		boolean isLocallyPresent = false;
			    		try {
			    			heartBeatResp=jTracker.heartBeat(heartBeatByte);
			    			printToScreen("heart beat responded");			    			
							HeartBeatResponse hbResp = HeartBeatResponse.parseFrom(heartBeatResp);
							hbResp.getStatus();
							noOfMFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getMapActiveCount();
							noOfRFreeWorkers = MasterTaskWorker.MAX_THREADS- masWorker.getReduceActiveCount();
							//printToScreen("After recieving maprequest, my free thread count is:"+noOfMFreeWorkers+":"+noOfRFreeWorkers);

							List<MapTaskInfo> mtiList = hbResp.getMapTasksList();
							if(mtiList!=null && !mtiList.isEmpty()){
							for(MapTaskInfo m: mtiList){
								for(BlockLocations block :m.getInputBlocksList()){
									isLocallyPresent=false;
									for(DataNodeLocation blockLocation: block.getLocationsList()){
										if(myIp.equalsIgnoreCase(blockLocation.getIp())){
											isLocallyPresent=true;
											printToScreen("the block "+ block.getBlockNumber()+"is locally present:");
											break;
										}
									}
									if(!isLocallyPresent){
										//the block is not there in local file system
										printToScreen("The block is not there in local file system.. Fetching from hdfs:"+block.getBlockNumber());
										fetchBlockToLocal(block.getBlockNumber(),block.getLocations(0).getIp(),block.getLocations(0).getPort());
										
									}
									for(DataNodeLocation blockLocation: block.getLocationsList()){
										String OutFileName = "job_"+   m.getJobId()+"map_"+m.getTaskId();
										String InputFileName = blocksDataPath+block.getBlockNumber();
										MapperWorker w = new MapperWorker(m.getJobId(), m.getTaskId(), mapOutputPath, OutFileName,
												InputFileName);
										addMapTaskToMap(m.getJobId(), m.getTaskId(), OutFileName);//Appending to localMap for processing
										masWorker.doExecuteMapper(w);
									}
								}
							}
							
							
							
							}
							List<ReducerTaskInfo> rtiList = hbResp.getReduceTasksList();
							if(rtiList!=null && !rtiList.isEmpty()){
                               for(ReduceTaskStatusData st : rTStatusMap.keySet()){
                            	   System.out.println(st.toString()+":"+st.hashCode());
                               }
								for(ReducerTaskInfo r:rtiList){
									int jobId = r.getJobId();
									
									int mapTaskId = r.getTaskId();
									ReduceTaskStatusData rMem = new ReduceTaskStatusData(jobId, mapTaskId,r.getMapOutputFilesList());
									if(rTStatusMap.containsKey(rMem)){
										System.out.println("Skipping:"+rMem.toString());
										continue;
									} 
										System.out.println("Submitting:"+r.getMapOutputFilesList());
										ReduceWorker w = new ReduceWorker(r.getMapOutputFilesList(),r.getJobId(),mapTaskId);
										addReduceTaskToMap(r.getJobId(), mapTaskId, r.getMapOutputFilesList());
										masWorker.doExecuteReducer(w);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
			    		System.out.println("\n\n");
			      }
			      
			    };
			    
			    ScheduledExecutorService service = Executors
			                    .newSingleThreadScheduledExecutor();
			    service.scheduleAtFixedRate(runnable, 0, 3, TimeUnit.SECONDS);
	}
	
	private void addMapTaskToMap(int jobId, int taskId, String mapOutputFile){
		MapTaskStatusData mtData = new MapTaskStatusData(jobId, taskId, mapOutputFile);
		mTStatusMap.put(mtData, false);
	}
	
	private void addReduceTaskToMap(int jobId, int taskId, List<String> fList){
		ReduceTaskStatusData mtData = new ReduceTaskStatusData(jobId, taskId, fList);
		System.out.println("Submitted reduced:"+mtData.toString());
		rTStatusMap.put(mtData, false);//check if synchronized is required or nots
	}
	
	protected static void completeMapTaskToMap(int jobId, int taskId){
		MapTaskStatusData mtData = new MapTaskStatusData(jobId, taskId);
		//`System.out.println("attempting to set to true for :"+mtData.getTaskId());
		synchronized (mTStatusMap) {
			mTStatusMap.replace(mtData, true);
		}
		
//		System.out.println("Set to true for :"+mtData.getTaskId());
	}
	
	protected static void completeReduceTaskToMap(int jobId, int taskId, List<String> fileList){
		ReduceTaskStatusData rtData = new ReduceTaskStatusData(jobId, taskId, fileList);
		synchronized (rTStatusMap) {
			rTStatusMap.replace(rtData,true);
		}
	}
	
	
	private IJobTracker establishConnectionWithJobTracker(String ipAddress, int portNo){
		IJobTracker jTracker = null;
		try {
			//System.out.println("IpAddress is:"+ipAddress);
			jTracker= (IJobTracker)Naming.lookup("rmi://"+ipAddress+":"+portNo+"/jTracker");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			return null;
		}
		//System.out.println("connection established with jtracker");
		return jTracker;
	}
	
	public void fetchBlockToLocal(int blockNum, String dataNodeIp, int port){
		IDataNode dataNode=establishConnectionWithDataNode(dataNodeIp,port);
		ReadBlockRequest.Builder rbrb = ReadBlockRequest.newBuilder();
    	rbrb.setBlockNumber(blockNum);
    	System.out.println("before calling dn");
    	byte[] rbRespStream=null;
		try {
			rbRespStream = dataNode.readBlock(rbrb.build().toByteArray());
	    	System.out.println("after calling dn");
	    	ReadBlockResponse rbresp = ReadBlockResponse.parseFrom(rbRespStream);
	    	System.out.println("The file is getting written to "+blocksDataPath+blockNum);
	    	//BufferedInputStream bis = new BufferedInputStream(rbresp.getData(0).toByteArray());
	    	byte []respByte = rbresp.getData(0).toByteArray();
	    	FileOutputStream fos = new FileOutputStream(new File(blocksDataPath+blockNum));
	    	fos.write(respByte);
	    	fos.close();
	    	//writeToFile(rbresp.getData(0),blockNum+"");

		} catch (RemoteException | InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		
	}
	private IDataNode establishConnectionWithDataNode(String ipAddress, int portNo){
		IDataNode dataNode = null;
		try {
			//System.out.println("IpAddress is:"+ipAddress);
			dataNode= (IDataNode)Naming.lookup("rmi://"+ipAddress+":"+portNo+"/dnserver");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			System.out.println("Not able to connect to NameNode");
			return null;
		}
		return dataNode;
	}
	
	public void writeToFile(String data,String fileName){
		try {
			
			FileWriter fw = new FileWriter(new File(blocksDataPath+fileName),true);
			fw.write(data);
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void printToScreen(String log){
		if(debug)
			System.out.println(log);
	}
	
}
