package test;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class MyClient {
	public static void main(String args[]){ 
		try { //String lookup = "rmi://192.168.56.1:1099/sonoo";
			Adder stub = (Adder) Naming.lookup("rmi://"+args[0]+":1099/sonoo");
			System.out.println("conn done");
			System.out.println(stub.add(45, 44));
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
