package taskTracker;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MasterWorker {
	
	public static final byte MAX_THREAD_COUNT = 3;
	private ExecutorService executor = null;
	List<Future<Byte>> fList = null;
	private byte freeSlots = MAX_THREAD_COUNT;
	
	
	public MasterWorker(){
		System.out.println("Master Worker is ready to recieve requests");
		fList = new LinkedList<Future<Byte>>();
		executor = Executors.newFixedThreadPool(MAX_THREAD_COUNT);
	}
	
	public byte getFreeSlots(){
		return freeSlots;
	}
	
	private void processJobTrackerRequest(){
		TaskTrackerWorker worker = new TaskTrackerWorker();
		freeSlots = (byte) (MAX_THREAD_COUNT - fList.size());
		fList.add(executor.submit(worker));
		
	}
}
