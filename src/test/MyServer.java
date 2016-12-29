package test;

import java.rmi.*;  
import java.rmi.registry.*;  
public class MyServer{  
public static void main(String args[]){   
try{  
Adder stub=new AdderRemote();  
Naming.rebind("rmi://"+args[0]+":1099/sonoo",stub);  
}catch(Exception e){System.out.println(e);}  
}  
}  