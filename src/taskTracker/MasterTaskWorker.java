package taskTracker;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import datanode.IDataNode;
import namenode.INameNode;
import protobuf.Hdfs.AssignBlockRequest;
import protobuf.Hdfs.AssignBlockResponse;
import protobuf.Hdfs.BlockLocationRequest;
import protobuf.Hdfs.BlockLocationResponse;
import protobuf.Hdfs.BlockLocations;
import protobuf.Hdfs.CloseFileRequest;
import protobuf.Hdfs.DataNodeLocation;
import protobuf.Hdfs.OpenFileRequest;
import protobuf.Hdfs.OpenFileResponse;
import protobuf.Hdfs.ReadBlockRequest;
import protobuf.Hdfs.ReadBlockResponse;
import protobuf.Hdfs.WriteBlockRequest;
import protobuf.Hdfs.WriteBlockResponse;

public class MasterTaskWorker {
	static final int MAX_THREADS = 3;
	ThreadPoolExecutor mapExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);
	ThreadPoolExecutor reduceExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
	
	int mapActiveCount = 0, reduceActiveCount = 0;

	TaskTrackerUtility utility_ = null;
	
	public MasterTaskWorker(TaskTrackerUtility utility){
		utility_ = utility;
  	}
	
	public int getMapActiveCount(){
		return mapExecutor.getActiveCount();
	}
	
	public int getReduceActiveCount(){
		return reduceExecutor.getActiveCount();
	}

	public void doExecuteMapper(MapperWorker w) {
		mapActiveCount = mapExecutor.getActiveCount();
		if(mapExecutor.getActiveCount() == MAX_THREADS){
			System.out.println("System is busy so cannot submit"+w.inputFileName);
			return;
		}
		mapExecutor.submit(w);
		System.out.println("Submitted:"+w.jobId+":"+w.taskId);
	}
	
	public void doExecuteReducer(ReduceWorker w) throws Exception{
		reduceActiveCount = reduceExecutor.getActiveCount();
		if(reduceExecutor.getActiveCount() ==MAX_THREADS){
			System.out.println("System is busy so cannot submit"+w.fileName);
			return;
		}
		reduceExecutor.submit(w);
	}
}

class MapperWorker implements Callable<Integer>{
    StringBuilder sb = new StringBuilder();
	String outputFileName = null;
	String inputFileName = null;
	String outputFilePath ="";
	int jobId = 0,taskId = 0;
	public MapperWorker( int jobId, int taskId, String outputFilePath,String outputFileName,String inputFileName){
		this.outputFilePath = outputFilePath;
		this.outputFileName=outputFileName;
		this.inputFileName = inputFileName;
		this.jobId = jobId;
		this.taskId = taskId;
	}
	
	private void doMapperLoading(){
		try{
				Class<?> c = Class.forName("mapper.Mapper");
			    Object t = c.newInstance();
			    Method m = c.getMethod("map",String.class);
			    try{
			    	sb.append("inputFileName in do mapper loading........"+inputFileName+"\n");

			    	@SuppressWarnings("resource")
					BufferedReader br = new BufferedReader(new FileReader(new File(inputFileName)));
			    	String line = null;
			    	StringBuilder fsb = new StringBuilder();
			    	while((line=br.readLine())!=null){
			    		Object args[] = {line};
			    		fsb.append((String)m.invoke(t, args));
			    		fsb.append("\n");
			    	}
			    	br.close();
			    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFilePath+outputFileName)));
			    	sb.append("Writing to file..........................:"+outputFileName+"\n");
			    	bw.write(fsb.toString());
			    	bw.close();
			    }catch(Exception ex){
			    	ex.printStackTrace();
			    	sb.append("Error in Mapper class execution\n");
			    }
			    TaskTrackerUtility.completeMapTaskToMap(jobId, taskId);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	private INameNode establishConnectionWithNameNode(String ipAddress, int portNo){
		INameNode nameNode = null;
		try {
			//System.out.println("IpAddress is:"+ipAddress);
			nameNode= (INameNode)Naming.lookup("rmi://"+ipAddress+":"+portNo+"/nnserver");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			System.out.println("Not able to connect to NameNode\n");
			return null;
		}
		return nameNode;
	}
	
	public void doHDFSForCompletedMapperTask(String mapOutputFileName){
		int startIndex = mapOutputFileName.lastIndexOf("/");
		String hdfsMapOutputFileName = mapOutputFileName.substring(startIndex+1, mapOutputFileName.length());
		sb.append("Writing to file in hdfs "+mapOutputFileName+"\n");
		INameNode nameNode = establishConnectionWithNameNode(TaskTrackerUtility.nameNodeIp,TaskTrackerUtility.nameNodePort);
		try {
			int handle=-1;
			byte[] ofReqStream =new byte[20000];
			OpenFileRequest.Builder ofb= OpenFileRequest.newBuilder();
			ofb.setFileName(hdfsMapOutputFileName);
			ofb.setForRead(false);
			ofb.build();
			ofReqStream =ofb.build().toByteArray();
			
			byte[] ofRespStream= nameNode.openFile(ofReqStream);		
		    OpenFileResponse ofresp =	OpenFileResponse.parseFrom(ofRespStream);
		    handle= ofresp.getHandle();
		    if(handle==-1){
		    	sb.append("Can't open the file\n");		    	
		    }
		    
		    AssignBlockRequest.Builder abr=AssignBlockRequest.newBuilder();
		    abr.setHandle(handle);
		    		    		   
		    File file = new File(mapOutputFileName);
		    FileInputStream is=null;
		    try {
				is = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		    
		    byte[] chunk = new byte[TaskTrackerUtility.blockSize];
		    IDataNode datanode=null;
		    int noOfByte = 0;
		    while ( (noOfByte = is.read(chunk)) != -1){
				    //System.out.println("Calling assign block");
				    byte[]  abrespStream = nameNode.assignBlock(abr.build().toByteArray());
				    AssignBlockResponse abresp = AssignBlockResponse.parseFrom(abrespStream);
				    int abRespStatus,blockNumber;
				    String ip=null;
				    int port;
				    abRespStatus=abresp.getStatus();
				    
				    protobuf.Hdfs.BlockLocations bl= abresp.getNewBlock();
				    blockNumber=bl.getBlockNumber();
				    
				    //get the first datanode location and pass it the chunk and the other data nodes info
				    //the first data node will cascade copy the chunk to other datanodes.
				    protobuf.Hdfs.DataNodeLocation dnl=bl.getLocations(0);						    						    
				    ip=dnl.getIp();
				    port=dnl.getPort();
				    //System.out.println("connecting to  "+ip+":"+port);
				    WriteBlockRequest.Builder wbreq = WriteBlockRequest.newBuilder();
				    wbreq.setBlockInfo(bl);
				    wbreq.addData(ByteString.copyFrom(chunk, 0, noOfByte));
				    
				   // if(datanode==null)
				    	//System.out.println("establising the conn");
					datanode = (IDataNode)Naming.lookup("rmi://"+ip+":"+port+"/dnserver");
					//System.out.println("conn established ");
					byte[] wbr = datanode.writeBlock(wbreq.build().toByteArray());
				    WriteBlockResponse wbresp = WriteBlockResponse.parseFrom(wbr);
				    int wbRespStatus= wbresp.getStatus();
		    }
		    sb.append("HDFS File Write is complete for :"+hdfsMapOutputFileName+"\n");
		    CloseFileRequest.Builder cfr = CloseFileRequest.newBuilder();
		    cfr.setHandle(handle );
		    nameNode.closeFile(cfr.build().toByteArray());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Integer call() {
		//System.out.println("Am executing: mapper"+taskId);
		sb.append("mapper starts for:"+jobId+":"+taskId+"\n");
		doMapperLoading();
		sb.append("Mapper Execution complete:"+jobId+":"+taskId+",Now initiating HDFS Put request\n");
		doHDFSForCompletedMapperTask(this.outputFilePath+this.outputFileName);
		System.out.println(sb.toString());
		sb.setLength(0);
		return taskId;
	}
}

class ReduceWorker implements Callable<Byte>{
	StringBuilder sb = new StringBuilder();
	String input = null;
	String fileName = null;
	int jobId;
	int reducerId;
	List<String> fileList = null;
	
	public ReduceWorker(String fileName,int jobId,int reducerId){
		this.fileName = fileName;
		this.reducerId=reducerId;
		this.jobId=jobId;
	}
	
	public ReduceWorker(List<String> fileList, int jobId, int reducerId){
		this.reducerId=reducerId;
		this.jobId=jobId;
		this.fileList = fileList;
	}
	
	private void doReducerLoading(String fileName,String reducerOutputFileName){
		try{
				Class<?> c = Class.forName("reducer.Reducer");
			    Object t = c.newInstance();
			    Method m = c.getMethod("reduce",String.class);
			    try{
			    	@SuppressWarnings("resource")
					BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
			    	String line = null;
			    	StringBuilder fsb = new StringBuilder();
			    	sb.append("Reducer reading from file "+fileName+"\n");
			    	
			    	while((line=br.readLine())!=null){
//			    		sb.append("Line read by reducer:"+line+"\n");
			    		Object args[] = {line};
//			    		sb.append("line sent to reducer:"+line);
			    		String l = (String)m.invoke(t, args);
			    		
			    		if(l==null)
			    			continue;
			    		fsb.append(l);
			    		fsb.append("\n");
			    	}
//			    	sb.append("File content:"+fileName+":"+fsb.toString()+"\n");
			    	br.close();
			    	if(fsb.toString().length()>0){
				    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File(TaskTrackerUtility.reducerOutputFolder + reducerOutputFileName),true));
//				    	sb.append("Content from reduce of :"+fileName+" is "+fsb.toString());
				    	bw.write(fsb.toString());
				    	fsb.setLength(0);
				    	bw.close();
			    	}
			    }catch(Exception ex){
			    	ex.printStackTrace();
			    	sb.append("Error in Reducer class execution");
			    }
			    sb.append("Reducer Completed for:"+fileName+"\n");
			    
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	private INameNode establishConnectionWithNameNode(String ipAddress, int portNo){
		INameNode nameNode = null;
		try {
			nameNode= (INameNode)Naming.lookup("rmi://"+ipAddress+":"+portNo+"/nnserver");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			sb.append("Not able to connect to NameNode");
			return null;
		}
		return nameNode;
	}

	
	public void fetchAndReduce(String fileName,int reducerId){
		sb.append("\n");
		try{
			int handle=-1;
			boolean isBlockLocallyPresent=false;
			byte[] ofReqStream =new byte[20000];
			OpenFileRequest.Builder ofb= OpenFileRequest.newBuilder();
			ofb.setFileName(fileName);
			ofb.setForRead(true);
			ofb.build();
			ofReqStream =ofb.build().toByteArray();
			INameNode namenode= establishConnectionWithNameNode(TaskTrackerUtility.nameNodeIp,TaskTrackerUtility.nameNodePort);//(INameNode)Naming.lookup("rmi://10.0.0.1:1099/nnserver");
			byte[] ofRespStream= namenode.openFile(ofReqStream);	
		    OpenFileResponse ofresp =	OpenFileResponse.parseFrom(ofRespStream);
		    handle= ofresp.getHandle();
		    if(handle==-1){
		    	sb.append("Can't open the file\n");		    	
		    }
		    else 
		    	sb.append("got the handle : "+handle+" for file:"+fileName+" with hashcode :"+this.hashCode()+"--"+new Date()+":"+System.currentTimeMillis()+"\n");
		    List<Integer> blockList = ofresp.getBlockNumsList();
		    sb.append("The block locations for the file:"+fileName+" are:"+blockList+"\n");
		    BlockLocationRequest.Builder blorb= BlockLocationRequest.newBuilder();
		    byte[] blockByte =new byte[20000];
		    for(Integer blockNumber:blockList){
		    	blorb.addBlockNums(blockNumber);
		    	sb.append("Sending request for :"+blockNumber+"\n");
		    }
		    blockByte = blorb.build().toByteArray();
		    byte[] blockLocations = namenode.getBlockLocations(blockByte);
		    BlockLocationResponse blockLocResp = BlockLocationResponse.parseFrom(blockLocations);
		    List<BlockLocations> loc = blockLocResp.getBlockLocationsList();
		    IDataNode datanode=null;
		    
		    for(BlockLocations l:loc){
		    	List<DataNodeLocation> dnlList= l.getLocationsList();
		    	isBlockLocallyPresent=new File(TaskTrackerUtility.blocksDataPath+l.getBlockNumber()).exists();
		    	
		    	/*for(DataNodeLocation dnl: dnlList){
		    		if(dnl.getIp().equalsIgnoreCase(TaskTrackerUtility.myIp)){
		    			sb.append("The block "+l.getBlockNumber()+" is locally present. no need to fetch\n");
		    			isBlockLocallyPresent=true;
		    			// copy it to intermediate folder from blocks folder
		    			doReducerLoading(TaskTrackerUtility.blocksDataPath+l.getBlockNumber(),"reducer"+reducerId+"output.txt");
		    		}
		    	}*/
		    	if(isBlockLocallyPresent) {
		    		doReducerLoading(TaskTrackerUtility.blocksDataPath+l.getBlockNumber(),"reducer"+reducerId+"output.txt");
		    		continue;
		    	}
		    	DataNodeLocation dnl = l.getLocations(0);
		    	dnl.getIp();
		    	datanode = (IDataNode)Naming.lookup("rmi://"+dnl.getIp()+":"+dnl.getPort()+"/dnserver");
		    	ReadBlockRequest.Builder rbrb = ReadBlockRequest.newBuilder();
		    	rbrb.setBlockNumber(l.getBlockNumber());
		    	sb.append("before calling dn\n");
		    	byte[] rbRespStream = datanode.readBlock(rbrb.build().toByteArray());
		    	sb.append("after calling dn\n");
		    	ReadBlockResponse rbresp = ReadBlockResponse.parseFrom(rbRespStream);
		    	byte []respByte = rbresp.getData(0).toByteArray();
		    	FileOutputStream fos = new FileOutputStream(new File(TaskTrackerUtility.intermediateFolder+l.getBlockNumber()));
		    	fos.write(respByte);
		    	fos.close();
		    	sb.append("Now starting reducer loading on :"+l.getBlockNumber()+"\n");
		    	doReducerLoading(TaskTrackerUtility.intermediateFolder+l.getBlockNumber(),"reducer"+reducerId+"output.txt");
		    	sb.append("dnl received in client "+dnl.getIp()+"  :"+dnl.getPort()+"\n");
		    }
		    
		}catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		sb.append("Reducer completed");
	}
	
	private boolean isBlockPresentInLocal(String fileName){
		return new File(fileName).exists();
	}
 
	public void writeToFile(String data,String fileName){
		try {
			FileWriter fw = new FileWriter(new File(TaskTrackerUtility.intermediateFolder+fileName),true);
			fw.write(data);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		sb.append("Writing to intermediate is complete\n");
	}
	
	@Override
	public Byte call() throws Exception {
		for(String fileName:fileList){
			fetchAndReduce(fileName,reducerId);
		}
		System.out.println("Reducer Logs:"+sb.toString());
		sb.setLength(0);
		System.out.println("Reducer Task completed:"+jobId+":"+reducerId+":"+this.fileList);
		System.out.println();
		TaskTrackerUtility.completeReduceTaskToMap(jobId, reducerId, fileList);
		return (byte)1;
	}
}
