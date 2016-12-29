package namenode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import datanode.IDataNode;
import protobuf.Hdfs.AssignBlockRequest;
import protobuf.Hdfs.AssignBlockResponse;
import protobuf.Hdfs.BlockLocationRequest;
import protobuf.Hdfs.BlockLocationResponse;
import protobuf.Hdfs.BlockLocations;
import protobuf.Hdfs.BlockReportRequest;
import protobuf.Hdfs.BlockReportResponse;
import protobuf.Hdfs.CloseFileResponse;
import protobuf.Hdfs.DataNodeLocation;
import protobuf.Hdfs.HeartBeatRequest;
import protobuf.Hdfs.HeartBeatResponse;
import protobuf.Hdfs.ListFilesRequest;
import protobuf.Hdfs.ListFilesResponse;
import protobuf.Hdfs.OpenFileRequest;
import protobuf.Hdfs.OpenFileResponse;

@SuppressWarnings("serial")
public class NameNodeServer extends UnicastRemoteObject implements INameNode {
	static final int REPLICATION_FACTOR=2;
	static String configFilesPath,nameNodeIp=null;
	static int maxBlockSeq,nameNodePort;
    String fileName = "";
    boolean debug=true;
    static AtomicInteger handle;
    static AtomicInteger blockNum;
    static HashMap<String,ArrayList<Integer>> filenblocks = new HashMap<String,ArrayList<Integer>>();
    HashMap<Integer,String> fileHandle = new HashMap<Integer,String>();
    static List<DataNode> dataNodesList = null;
    // blockData stores the list of block numbers against the data node parameters {id,ip,port}
    static Map<Integer,Set<DataNode>> blockData = new HashMap<Integer,Set<DataNode>>();
    private HashMap<Integer,List<Integer>> dataNodeBlockList = new HashMap<Integer,List<Integer>>();
    
	protected NameNodeServer() throws RemoteException {
		super();
		
 	}
	

	@SuppressWarnings("resource")
	public  static void instantiate(){
		BufferedReader br = null;
		try {
			
		     //----------------------------------------------------------------------------------------
			 // Load ip from text file
			String sCurrentLine;
			br = new BufferedReader(new FileReader(configFilesPath+"namenodeip.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				nameNodeIp=sCurrentLine.split(":")[0];
				nameNodePort=Integer.parseInt(sCurrentLine.split(":")[1].trim());
				break;
			}
			
		     //----------------------------------------------------------------------------------------
			// Load the filename: Block List into memory from nnconfig.txt
				br = new BufferedReader(new FileReader(configFilesPath+"nnconfig.txt"));
				
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
			
	     //----------------------------------------------------------------------------------------
		 // Load datanodes info into the memory
				
				dataNodesList = new ArrayList<DataNode>();
				br = new BufferedReader(new FileReader(configFilesPath+"nodesinfo.txt"));
				while ((sCurrentLine = br.readLine()) != null) {
				
					String[] nodesstr = sCurrentLine.split(":");
					
					int id = Integer.parseInt(nodesstr[0].trim());
					String ip=nodesstr[1].trim();
					int port=Integer.parseInt(nodesstr[2].trim());
					
					DataNode dn= new DataNode(id,ip,port);
					dataNodesList.add(dn);
				}
				br.close();
	   //---------------------------------------------------------------
				int i=getMaxBlockNum();
				blockNum = new AtomicInteger(i);
				handle= new AtomicInteger(0);
				
		} catch (IOException e) {
			e.printStackTrace();
		} 
		//System.out.println("filenblocks is "+filenblocks);
	}
	
	public static void main(String args[]){
		if(args.length==1)
		  configFilesPath = args[0];
		NameNodeServer nnserver;
		try { instantiate();
			nnserver = new NameNodeServer();
			System.out.println("Name node starting on "+nameNodeIp+nameNodePort);
			Naming.rebind("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver", nnserver);
			System.out.println("Name node server started...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {

		byte[] retArr= null;
		OpenFileRequest ofr =null;
		
		try { 
			ofr = OpenFileRequest.parseFrom(inp);
			OpenFileResponse.Builder ofrb = OpenFileResponse.newBuilder();
			ofrb.setHandle(handle.incrementAndGet());
			this.fileHandle.put(handle.get(),ofr.getFileName());
			if(ofr.getForRead()){
			//System.out.println("filenblocks is , fileName"+filenblocks+ofr.getFileName());
			ofrb.addAllBlockNums(filenblocks.get(ofr.getFileName()));
			}
			retArr=ofrb.build().toByteArray();
			printToScreen("Received request to open the file :"+fileName+":and handle gave is:"+handle.get());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
		return retArr;
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		PrintWriter writer;
		String blockNums="";
		try {
			writer = new PrintWriter(configFilesPath+"nnconfig.txt","UTF-8");
			for (Map.Entry<String, ArrayList<Integer>> entry : filenblocks.entrySet()){
				writer.print(entry.getKey()+" : ");
				for(Integer i : entry.getValue()){
					blockNums = blockNums + i + "," ;
				}
				blockNums = blockNums.substring(0, blockNums.length()-1);
				writer.print(blockNums);
				writer.println();
				blockNums="";
			}
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		CloseFileResponse.Builder cfresp = CloseFileResponse.newBuilder();
		cfresp.setStatus(1);
		return cfresp.build().toByteArray();
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {

		BlockLocationResponse.Builder blockLocationResponse = null;
		blockLocationResponse =  BlockLocationResponse.newBuilder();
		try{
			
			BlockLocationRequest blockLocationRequest = BlockLocationRequest.parseFrom(inp);
			List<Integer> blockNumberList = blockLocationRequest.getBlockNumsList();
		//	System.out.println("list size is "+blockNumberList.size());
			for(Integer blockNumber:blockNumberList){
				//System.out.println(blockNumber+","+blockData);
			//	System.out.println("Before returning the blockLocation Request:"+blockData);
				Set<DataNode> dataNodes = blockData.get(blockNumber);
				BlockLocations.Builder blockLocation =BlockLocations.newBuilder();
				for(DataNode node:dataNodes){
					
						DataNodeLocation.Builder dataNode  = DataNodeLocation.newBuilder();
						dataNode.setIp(node.getIp());
	                    dataNode.setPort(node.getPort());
	                    blockLocation.addLocations(dataNode);
	                    blockLocation.setBlockNumber(blockNumber);
					
					blockLocationResponse.addBlockLocations(blockLocation);
			}
			}
		}catch(Exception ex){
			
		}
		return blockLocationResponse.build().toByteArray();
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		byte[] abRespStream=null;
		int handle;
		String fileName=null;
		
		
			try {
				AssignBlockRequest abr = AssignBlockRequest.parseFrom(inp);
				handle=abr.getHandle();
				fileName=this.fileHandle.get(handle);
				printToScreen("Assigning the block "+blockNum.get());
					BlockLocations.Builder blb=BlockLocations.newBuilder();
					blb.setBlockNumber(blockNum.incrementAndGet());
					
					Collections.shuffle(dataNodesList);
					DataNode dn=null;
					for(int i=0;i<REPLICATION_FACTOR;i++){
					//	System.out.println("assign block req for "+this.fileName+" and handle is "+this.handle);
						DataNodeLocation.Builder dnlb= DataNodeLocation.newBuilder();
						dn=dataNodesList.get(i);
						printToScreen("Namenode requesting the client to replicate the block "+blockNum.get()+"on the node"+dn.getIp());
						dnlb.setIp(dn.getIp());
						dnlb.setPort(dn.getPort());										
						
					
						blb.addLocations(dnlb);
					}
					AssignBlockResponse.Builder abrb = AssignBlockResponse.newBuilder();
					abrb.setStatus(1);
					abrb.setNewBlock(blb);
					
					abRespStream= abrb.build().toByteArray();
					
					// add the block seq to the filenblocks
					ArrayList<Integer> fileBlockList=null;
					printToScreen("Updating config file for "+fileName);
					if(filenblocks.containsKey(fileName)){
						fileBlockList= filenblocks.get(fileName);
						fileBlockList.add(blockNum.get());
					}
					else{
						fileBlockList = new ArrayList<Integer>();
						fileBlockList.add(blockNum.get());
						filenblocks.put(fileName, fileBlockList);
					}
				
			//	}
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			

		return abRespStream;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		ListFilesResponse.Builder lfrb=null;
		try {
			ListFilesRequest lfr = ListFilesRequest.parseFrom(inp);
			String dir = lfr.getDirName();
			 lfrb = ListFilesResponse.newBuilder();
			lfrb.addAllFileNames(filenblocks.keySet());
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	return lfrb.build().toByteArray();
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		try { 
			BlockReportRequest brr = BlockReportRequest.parseFrom(inp);
			List<Integer> blockNumList=brr.getBlockNumbersList();
			DataNodeLocation dnl= brr.getLocation();
			dnl.getIp();
			dnl.getPort();
	//		System.out.println("Receieved block report from datanode "+dnl.getIp());
			dataNodeBlockList.put(brr.getId(), blockNumList);
			for(int blockNum : blockNumList){
				if(blockData.containsKey(blockNum)){
					blockData.get(blockNum).add(new DataNode(brr.getId(),dnl.getIp(),dnl.getPort()));
				}else{
					Set<DataNode> dns = new HashSet<DataNode>();
					dns.add(new DataNode(brr.getId(),dnl.getIp(),dnl.getPort()));
					blockData.put(blockNum, dns);
				}
			}
			NameNodeUtility nnUtil = new NameNodeUtility();
			nnUtil.flushDataNodeInfo(blockData, configFilesPath+"blockData.txt");
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		HeartBeatResponse.Builder hbresp=null;
		int id;
		 try {
			HeartBeatRequest hbrStream = HeartBeatRequest.parseFrom(inp);
			id=hbrStream.getId();
			hbresp= HeartBeatResponse.newBuilder();
			hbresp.setStatus(id);
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}		 
		return hbresp.build().toByteArray();
	}
	
	public static int getMaxBlockNum(){
		int max=-1,temp;
		for (Map.Entry<String, ArrayList<Integer>> entry : filenblocks.entrySet())
		{
		   // System.out.println(entry.getKey() + "/" + entry.getValue());
		    temp= Collections.max(entry.getValue());
		    if(temp>max) max=temp;
		}
		return max;
	}
	 
	 public synchronized DataNode [] getBlockLocations(Integer blockNo){
	        Set<DataNode> nodes  =  blockData.get(blockNo);
	        DataNode [] nodeList = new DataNode[nodes.size()];
	        nodeList = nodes.toArray(nodeList);
	        return nodeList;
	    }
	 
		public void printToScreen(String log){
			if(debug)
				System.out.println(log);
		}
}
