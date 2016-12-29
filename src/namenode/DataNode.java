package namenode;


public class DataNode {
	 private int id;
	    private String ip;
	    private int port;
	     
	     
	    public DataNode(int id,String ip,int port)
	    {
	        this.id = id;
	        this.ip = ip;
	        this.port = port;
	    }
	     
	     
	    public int getId() {
	        return id;
	    }
	    @Override
	    public boolean equals(Object obj) 
	    {
	        if(obj instanceof DataNode)
	        {
	            if(id == ((DataNode)obj).getId())
	            {
	                return true;
	            }
	        }
	        return false;
	    }
	    
	    public int hashCode(){
	    	return id;
	    }
	 
	 
	    public void setId(int id) {
	        this.id = id;
	    }
	    public String getIp() {
	        return ip;
	    }
	    public void setIp(String ip) {
	        this.ip = ip;
	    }
	    public int getPort() {
	        return port;
	    }
	    public void setPort(int port) {
	        this.port = port;
	    }
}