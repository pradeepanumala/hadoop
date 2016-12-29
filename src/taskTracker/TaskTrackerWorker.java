package taskTracker;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

public class TaskTrackerWorker implements Callable<Byte> {
	
	public TaskTrackerWorker(){
		super();
	}

	private void doMapping(){
		try{
				Class<?> c = Class.forName("mapper.Mapper");
			    Object t = c.newInstance();
			    Method m = c.getMethod("map",String.class);
			    Object args[] = {"abc"};
			    m.invoke(t, args);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	@Override
	public Byte call() {
		// TODO Auto-generated method stub
		try{
			Thread.sleep(1000);
			System.out.println("Task is completed");
		}catch(Exception e){
			System.out.println("Task is falied");
			return (byte)0;
		}
		return (byte)1;
	}
}
