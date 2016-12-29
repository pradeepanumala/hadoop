package datanode;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import namenode.INameNode;
import protobuf.Hdfs.BlockLocations;
import protobuf.Hdfs.BlockReportRequest;
import protobuf.Hdfs.DataNodeLocation;
import protobuf.Hdfs.HeartBeatRequest;
import protobuf.Hdfs.ReadBlockRequest;
import protobuf.Hdfs.ReadBlockResponse;
import protobuf.Hdfs.WriteBlockRequest;
import protobuf.Hdfs.WriteBlockResponse;
import utility.Util;

@SuppressWarnings("serial")
public class DataNodeServer extends UnicastRemoteObject implements IDataNode{
	Util util=null;
	static final int blockSize=100;
	static String nameNodeIp,configFilesPath=null;
	static int nameNodePort;
	 String ip,filesPath,blockFilesPath="";
	 int port,id=-1;
	 boolean debug;
	 Set<Integer> blockNumbers = null;
	 protected DataNodeServer() throws RemoteException {
		super();
	}
	private void instantiate() {
		
		BufferedReader br = null;
		try{
			String line;
			printToScreen("Loading the data node configuration from "+configFilesPath+"dnconfig.txt");
			br = new BufferedReader(new FileReader(configFilesPath+"dnconfig.txt"));
			while ((line = br.readLine()) != null) {
				String key=line.split(":")[0];
				String value=line.split(":")[1];
				if(key.equalsIgnoreCase("nameNodeIp"))
					nameNodeIp=value;
				else if (key.equalsIgnoreCase("nameNodePort"))
					nameNodePort=Integer.parseInt(value);
				else if (key.equalsIgnoreCase("ip"))
					this.ip=value;
				else if (key.equalsIgnoreCase("port"))
					this.port=Integer.parseInt(value);
				else if (key.equalsIgnoreCase("filesPath"))
					this.filesPath=value;
				else if (key.equalsIgnoreCase("blockFilesPath"))
					this.blockFilesPath=value;
			}
		}catch(Exception e){
			
		}
			
		util = new Util();
		loadBlockNumbers();
		
		//scheduleHeartBeatReport();
		scheduleBlockReport();
		
	}		
	
		
	public static void main(String args[]){
		if(args.length==1)
			  configFilesPath = args[0];
		DataNodeServer dnserver;
		try {
			dnserver = new DataNodeServer();
			dnserver.instantiate();
			Naming.rebind("rmi://"+dnserver.ip+":"+dnserver.port+"/dnserver", dnserver);
			System.out.println("DataNode Server Started...");
						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		int blockNumber = 0;
		ReadBlockResponse.Builder rbresp = null;
		
		FileInputStream is=null;
		printToScreen("readBlock invoked .. ");
		try {
			ReadBlockRequest rbr = ReadBlockRequest.parseFrom(inp);
			blockNumber=rbr.getBlockNumber();
			File f = new File(blockFilesPath+blockNumber);
			byte block[] = new byte[(int)f.length()];
			is =new FileInputStream(f);
		
			is.read(block);
			rbresp = ReadBlockResponse.newBuilder();
			rbresp.addData(ByteString.copyFrom(block));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rbresp.build().toByteArray();
	}

	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		System.out.println("WriteBlock Request on Datanode");
		IDataNode datanode=null;
		WriteBlockResponse.Builder wbresp = WriteBlockResponse.newBuilder();
		String blockFileLoc=filesPath+"blocksInfo.txt";
		try {
				WriteBlockRequest wbr =WriteBlockRequest.parseFrom(ByteString.copyFrom(inp));
				BlockLocations bl= wbr.getBlockInfo();
				
				BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(blockFilesPath+bl.getBlockNumber()));
				byte[] bytes = wbr.getData(0).toByteArray();
				bos.write(bytes);
				bos.close();
				printToScreen("writing to bloksinfo "+wbr.getBlockInfo().getBlockNumber()); 
				util.writeToFile(wbr.getBlockInfo().getBlockNumber()+"\n",blockFileLoc );
				printToScreen("block locatins count is "+bl.getLocationsCount());
				
				List<DataNodeLocation> dnllist = new ArrayList<DataNodeLocation>();
				for(int k=1;k<bl.getLocationsCount();k++){
					dnllist.add(bl.getLocations(k));
				}
				if(dnllist.size()>0){
					BlockLocations.Builder bl1= BlockLocations.newBuilder();
					bl1.setBlockNumber(bl.getBlockNumber());
					bl1.addAllLocations(dnllist);
					DataNodeLocation dnl=bl1.getLocations(0);
					String ip = dnl.getIp();
					int port= dnl.getPort();
					printToScreen("Calling the other datanode for writing block "+bl.getBlockNumber()+" onto it "+ip+":"+port);

					 WriteBlockRequest.Builder wbreq = WriteBlockRequest.newBuilder();
					 wbreq.setBlockInfo(bl1);
					    wbreq.addData(wbr.getData(0));	
					    datanode = (IDataNode)Naming.lookup("rmi://"+ip+":"+port+"/dnserver");
						byte[] wbr2 = datanode.writeBlock(wbreq.build().toByteArray());
				//	    WriteBlockResponse wbresp = WriteBlockResponse.parseFrom(wbr);
					//    int wbRespStatus= wbresp.getStatus();
				}
							
							
		} catch (IOException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	System.out.println("inp received in dn is "+output);
		wbresp.setStatus(1);
		return wbresp.build().toByteArray();
	}
	
	
	public void scheduleHeartBeatReport(){
		System.out.println("Heart Beat Started");
		Runnable runnable = new Runnable() {
		      public void run() {
		        // task to run goes here
		    	  doHeartBeat();
		    	  //System.out.println("heart beat report"+System.currentTimeMillis());
		      }
		    };
		    
		    ScheduledExecutorService service = Executors
		                    .newSingleThreadScheduledExecutor();
		    service.scheduleAtFixedRate(runnable, 0, 1, TimeUnit.SECONDS);
	}
	
	public void scheduleBlockReport(){
		System.out.println("Block report started");
		Runnable runnable = new Runnable() {
		      public void run() {
		        // task to run goes here
		    	  doBlockReport();
		          //System.out.println("block report"+System.currentTimeMillis());
		      }
		    };
		    
		    ScheduledExecutorService service = Executors
		                    .newSingleThreadScheduledExecutor();
		    service.scheduleAtFixedRate(runnable, 0, 1, TimeUnit.SECONDS);
	}
	
	private void doBlockReport(){
		DataNodeLocation.Builder dnlb = DataNodeLocation.newBuilder();
		dnlb.setIp(this.ip);
		dnlb.setPort(this.port);
		
		BlockReportRequest.Builder brrb = BlockReportRequest.newBuilder();
		brrb.setLocation(dnlb);
		brrb.setId(this.id);
		
		loadBlockNumbers();
		
		for(int i:blockNumbers){
			//System.out.println("block num "+i);
			brrb.addBlockNumbers(i);
		}
		
		INameNode namenode;
		try {
		//     System.out.println("establishing connection to "+nameNodeIp);
			namenode = (INameNode)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver");
			namenode.blockReport(brrb.build().toByteArray());
		//	System.out.println("block report sent");
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private void doHeartBeat(){
		INameNode namenode;
		HeartBeatRequest.Builder hbr = HeartBeatRequest.newBuilder();
		hbr.setId(1);
		
		try {
			namenode = (INameNode)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/nnserver");
			namenode.heartBeat(hbr.build().toByteArray());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private void loadBlockNumbers(){
		try{
		blockNumbers=util.getBlockNumbersFromFile(filesPath+"blocksInfo.txt");
		} catch(Exception e){
			System.out.println("file empty");
		}
	}
	
	public void printToScreen(String log){
		if(debug)
			System.out.println(log);
	}
	

}
