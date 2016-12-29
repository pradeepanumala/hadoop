package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;
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
import protobuf.Hdfs.ListFilesRequest;
import protobuf.Hdfs.ListFilesResponse;
import protobuf.Hdfs.OpenFileRequest;
import protobuf.Hdfs.OpenFileResponse;
import protobuf.Hdfs.ReadBlockRequest;
import protobuf.Hdfs.ReadBlockResponse;
import protobuf.Hdfs.WriteBlockRequest;
import protobuf.Hdfs.WriteBlockResponse;

public class Client {
	 static final int BLOCK_SIZE=500;
	 String nameNodeIp=null;
	 int nameNodePort;
	 String getOutputPath="";
	 boolean debug;
	public Client(String configFilesPath){
		
		String line = null;
		
		try { @SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(configFilesPath+"clientConfig.txt"));
			while ((line = br.readLine()) != null) {
				String key = line.split(":")[0];
				if(key.equalsIgnoreCase("nameNodeIp")){
					nameNodeIp=line.split(":")[1];
				}
				else if(key.equalsIgnoreCase("nameNodePort")){
					nameNodePort= Integer.parseInt(line.split(":")[1]);
				}
				else if(key.equalsIgnoreCase("getOutputPath")){
					getOutputPath=line.split(":")[1];
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	 }
	 public void get(String fileName){
			try{
				int handle=-1;
				byte[] ofReqStream =new byte[20000];
				OpenFileRequest.Builder ofb= OpenFileRequest.newBuilder();
				ofb.setFileName(fileName);
				ofb.setForRead(true);
				ofb.build();
				ofReqStream =ofb.build().toByteArray();
				INameNode namenode= (INameNode)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver");

				byte[] ofRespStream= namenode.openFile(ofReqStream);	
			    OpenFileResponse ofresp =	OpenFileResponse.parseFrom(ofRespStream);
			    handle= ofresp.getHandle();
			    if(handle==-1){
			    	System.out.println("Cannot open the file");		    	
			    }
			    else printToScreen("Handle for the file "+ fileName +" is "+handle);
			    List<Integer> blockList = ofresp.getBlockNumsList();
			    printToScreen("The block locations for the file "+ fileName +": "+blockList);
			    
			    BlockLocationRequest.Builder blorb= BlockLocationRequest.newBuilder();
			    byte[] blockByte =new byte[20000];
			    for(Integer blockNumber:blockList){
			    	blorb.addBlockNums(blockNumber);
			    	System.out.println("Sending request for :"+blockNumber);
			    }
			    blockByte = blorb.build().toByteArray();
			    byte[] blockLocations = namenode.getBlockLocations(blockByte);
			    BlockLocationResponse blockLocResp = BlockLocationResponse.parseFrom(blockLocations);
			    List<BlockLocations> loc = blockLocResp.getBlockLocationsList();
			    //System.out.println(loc);
			    IDataNode datanode=null;
			    
			    for(BlockLocations l:loc){
			    	DataNodeLocation dnl = l.getLocations(0);
			    	printToScreen("Fetching the block"+ l.getBlockNumber() +" from "+dnl.getIp());
			    	datanode = (IDataNode)Naming.lookup("rmi://"+dnl.getIp()+":"+dnl.getPort()+"/dnserver");
			    	ReadBlockRequest.Builder rbrb = ReadBlockRequest.newBuilder();
			    	rbrb.setBlockNumber(l.getBlockNumber());
			    	printToScreen("before calling dn");
			    	byte[] rbRespStream = datanode.readBlock(rbrb.build().toByteArray());
			    	printToScreen("after calling dn");
			    	ReadBlockResponse rbresp = ReadBlockResponse.parseFrom(rbRespStream);
			    	byte []respByte = rbresp.getData(0).toByteArray();
			    	FileOutputStream fos = new FileOutputStream(new File(""+getOutputPath+fileName),true);
			    	fos.write(respByte);
			    	fos.close();
			    	
			    	//writeToFile(rbresp.getData(0).toStringUtf8(),fileName);
			    	 
			    	printToScreen("dnl received in client "+dnl.getIp()+"  :"+dnl.getPort());
			    }
			    System.out.println("The file "+fileName+" fetched successfully");
			    
			}catch (MalformedURLException | RemoteException | NotBoundException e) {
				e.printStackTrace();
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
	 
	 
	public void put(String fileName){
		String absFileName = fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
		try {
			int handle=-1;
			byte[] ofReqStream =new byte[20000];
			OpenFileRequest.Builder ofb= OpenFileRequest.newBuilder();
			ofb.setFileName(absFileName);
			ofb.setForRead(false);
			ofb.build();
			
			
			ofReqStream =ofb.build().toByteArray();
			INameNode namenode= (INameNode)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver");
			
			byte[] ofRespStream= namenode.openFile(ofReqStream);		
		    OpenFileResponse ofresp =	OpenFileResponse.parseFrom(ofRespStream);
		    handle= ofresp.getHandle();
		    if(handle==-1){
		    	System.out.println("Can't open the file");		    	
		    }
		    
		    AssignBlockRequest.Builder abr=AssignBlockRequest.newBuilder();
		    abr.setHandle(handle);
		    		    		   
		    File file = new File(fileName);
		    FileInputStream is=null;
		    try {
				is = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		    
		    byte[] chunk = new byte[BLOCK_SIZE];
		    IDataNode datanode=null;
		    int noOfBytesRead = 0;
		    while ( (noOfBytesRead = is.read(chunk)) != -1){
		    		printToScreen("Calling assign block");
				    byte[]  abrespStream = namenode.assignBlock(abr.build().toByteArray());
				    AssignBlockResponse abresp = AssignBlockResponse.parseFrom(abrespStream);
				    int abRespStatus,blockNumber;
				    String ip=null;
				    int port;
				    abRespStatus=abresp.getStatus();
				    
				    BlockLocations bl= abresp.getNewBlock();
				    blockNumber=bl.getBlockNumber();
				    
				    List<DataNodeLocation> dnlList = bl.getLocationsList();
				    for(DataNodeLocation d : dnlList)
				    	printToScreen("the dn .. "+d.getIp());
				    //get the first datanode location and pass it the chunk and the other data nodes info
				    //the first data node will cascade copy the chunk to other datanodes.
				    DataNodeLocation dnl=bl.getLocations(0);						    						    
				    ip=dnl.getIp();
				    port=dnl.getPort();
				    printToScreen("The block "+bl.getBlockNumber()+" will be first put onto  "+ip+":"+port);
				    printToScreen("The size of block locs is "+bl.getLocationsCount());
				   // System.out.println("and then onto "+bl.getLocations(1).getIp());
				    
				    WriteBlockRequest.Builder wbreq = WriteBlockRequest.newBuilder();
				    wbreq.setBlockInfo(bl);
				    wbreq.addData(ByteString.copyFrom(chunk,0,noOfBytesRead));
				    
				   // if(datanode==null)
					datanode = (IDataNode)Naming.lookup("rmi://"+ip+":"+port+"/dnserver");
					System.out.println("conn established ");
					byte[] wbr = datanode.writeBlock(wbreq.build().toByteArray());
				    WriteBlockResponse wbresp = WriteBlockResponse.parseFrom(wbr);
				    int wbRespStatus= wbresp.getStatus();
				    if(wbRespStatus==1){
				    //	System.out.println("The file "+fileName+" is successfully written to the HDFS");
				    }
				    // if wbRespStatus = 1 then success, else failure
				    // need to find out what should be done if resp status is not 1
				    
						    
		    }
		    CloseFileRequest.Builder cfr = CloseFileRequest.newBuilder();
		    cfr.setHandle(handle );
		    namenode.closeFile(cfr.build().toByteArray());
		    
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void list(String Dir){
		ListFilesRequest.Builder lfrb = ListFilesRequest.newBuilder();
		lfrb.setDirName(Dir);
		INameNode namenode;
		try {
			namenode = (INameNode)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver");
			byte[] lfRespStream= namenode.list(lfrb.build().toByteArray());
			ListFilesResponse lfResp = ListFilesResponse.parseFrom(lfRespStream);
			List<String> fileList = new ArrayList<String>();
			fileList=lfResp.getFileNamesList();
			for(String file:fileList){
				System.out.println(file);
			}

		} catch (MalformedURLException | RemoteException | NotBoundException | InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}
	public void writeToFile(String data,String fileName){
		try {
			FileWriter fw = new FileWriter(new File(getOutputPath+fileName),true);
			fw.write(data);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String args[]) throws IOException{
		String command="put";
		String fileName=null;
		String configFilesPath="/home/pradeep/hdfs/configFiles/";
		if(args.length>0)
		 command=args[0];
		if(args.length>1)
		 fileName=args[1];
		if(args.length>2)
			configFilesPath=args[2];
		
		Client c = new Client(configFilesPath);
		
		if(command.equalsIgnoreCase("put"))
			c.put(fileName);
		else if (command.equalsIgnoreCase("get"))
			c.get(fileName);
		else if (command.equalsIgnoreCase("list"))
			c.list("/home/pradeep/hdfs/putFiles");
		else
			System.out.println("Invalid input");
		
	}
	
	public void printToScreen(String log){
		if(debug)
			System.out.println(log);
	}

}
