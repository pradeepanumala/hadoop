package utility;
import java.io.*;
import java.util.*;

public class Util {
public Set<Integer> getBlockNumbersFromFile(String fileName){
		Set<Integer> blockNumbers = new HashSet<Integer>();
		FileReader fr;
		try {
			fr = new FileReader(new File(fileName));
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			StringBuilder sb = new StringBuilder();
			while((line=br.readLine())!=null){
				 if (line=="") continue;
				blockNumbers.add(Integer.parseInt(line));
			}
		} catch ( Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return blockNumbers;
	}
	
public void writeToFile(String data,String fileName){
	try {
		System.out.println("...writin to ..."+fileName+"..data.."+data);
		FileWriter fw = new FileWriter(new File(fileName),true);
		fw.write(data);
		fw.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
	 public static void main(String[] args) {
		 
	  }

}
