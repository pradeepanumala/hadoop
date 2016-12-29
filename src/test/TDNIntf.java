package test;


import java.rmi.Remote;
import java.rmi.RemoteException;



	public interface TDNIntf extends Remote{

		String sayHello(byte[] inp) throws RemoteException;
		
		
	}