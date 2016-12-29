package test;

import java.rmi.Naming;


public class TClient {
		public static void main(String args[]){
	try{
		TDNIntf datanode = (TDNIntf)Naming.lookup("rmi://10.0.0.2:1099/tdnserver");
		System.out.println(".."+datanode.sayHello(null));
	}catch(Exception e){
		e.printStackTrace();
	}
	}
}
