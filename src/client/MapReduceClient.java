package client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import com.google.protobuf.InvalidProtocolBufferException;

import namenode.IJobTracker;
import namenode.JobTracker;
import protobuf.MapReduce.JobStatusRequest;
import protobuf.MapReduce.JobStatusResponse;
import protobuf.MapReduce.JobSubmitRequest;
import protobuf.MapReduce.JobSubmitResponse;

public class MapReduceClient {
	
	static String nameNodeIp,configFilesPath=null;
	static int nameNodePort;
	
	public MapReduceClient(){
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
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		String mapName,reducerName,inputFileName,outputFileName;
		int numReducers;
		if(args.length<5) 
				System.exit(1);
		 	mapName=args[0];
		 	reducerName=args[1];
		 	inputFileName=args[2];
		 	outputFileName=args[3];
		 	numReducers=Integer.parseInt(args[4]);
			configFilesPath = args[5];
		MapReduceClient client = new MapReduceClient();
		client.submitJob(mapName,reducerName,inputFileName,outputFileName,numReducers);
	}
	
	public void submitJob(String MapName,String ReducerName,String InputFile,String OutputFile,int NumReduceTasks){
		IJobTracker jtracker=null;
		int jobId;
		byte byteResp[];
		

		try {
			System.out.println("Getting the jobtracker handle "+nameNodeIp+":"+nameNodePort);
			jtracker = (IJobTracker)Naming.lookup("rmi://"+nameNodeIp+":"+nameNodePort+"/jTracker");
			JobSubmitRequest.Builder jrbStream = JobSubmitRequest.newBuilder();
			jrbStream.setInputFile(InputFile);
			jrbStream.setMapName(MapName);
			jrbStream.setReducerName(ReducerName);
			jrbStream.setOutputFile(OutputFile);
			jrbStream.setNumReduceTasks(NumReduceTasks);
			
			byteResp=jtracker.jobSubmit(jrbStream.build().toByteArray());
			
			JobSubmitResponse jsresp = JobSubmitResponse.parseFrom(byteResp);
			jobId=jsresp.getJobId();
			System.out.println("Job submitted successfully - job id is "+jobId);
			
			JobStatusRequest.Builder jsrb = JobStatusRequest.newBuilder();
			jsrb.setJobId(jobId);
			while(true){
				try{
					Thread.sleep(2000);
				}catch(Exception ex){
					
				}
				byte[] jobStatRespBuilder =jtracker.getJobStatus(jsrb.build().toByteArray());
				JobStatusResponse  JobStatusResp = JobStatusResponse.parseFrom(jobStatRespBuilder);
					
				
				System.out.println("The status of the job is "+JobStatusResp.getStatus());
				System.out.println("The Total Map Tasks :"+JobStatusResp.getTotalMapTasks());
				System.out.println("The Total Reduce Tasks :"+NumReduceTasks);
				System.out.println("The Reduce tasks started : "+JobStatusResp.getNumReduceTasksStarted());
				if(JobStatusResp.getJobDone()){
					System.out.println("The Map Reduce is succesfully done for the file "+InputFile);
					break;
				}
				System.out.println("Job is still in progress\n\n\n\n........");

			}
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}

}
