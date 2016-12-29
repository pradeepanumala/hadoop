package test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.WireFormat.JavaType;

public class Test {
	public static void main(String args[]) throws InstantiationException, IllegalAccessException{
	
		String line = "00000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa333434354545";
		try{
			 if(line.length()>0 && line.split(":")[1].trim().equalsIgnoreCase("1")){
				 //	 System.out.println("Reducer output line is:"+line);
				 System.out.println(line);
			 }
			}catch(Exception e){
			//	System.out.println("The exception occured for line "+line);
			//	e.printStackTrace( );
			}
	
	}
}
