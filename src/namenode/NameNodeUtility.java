package namenode;

import java.io.*;
import java.util.*;

public class NameNodeUtility {
	
	public synchronized void flushDataNodeInfo(Map<Integer, Set<DataNode>> blockData, String filePath){
		try{
			FileWriter fw = new FileWriter(new File(filePath));
			BufferedWriter br = new BufferedWriter(fw);
			StringBuilder sb = new StringBuilder();
			//System.out.println("bd size "+blockData.size());
			for(Integer block:blockData.keySet()){
				//System.out.println("block num "+block);
				sb.append(block+"-");
				Set<DataNode> dn = blockData.get(block);
				for(DataNode n:dn){
					sb.append(n.getIp()+":"+n.getPort()+",");
				}
				br.write(sb.substring(0,sb.length()-1));
				br.newLine();
				sb.setLength(0);
			} br.close();
		//	System.out.println("Closing stream");
			fw.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}

}
