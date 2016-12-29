package reducer;

public class Reducer implements IReducer {

	@Override
	public String reduce(String line) {
		//System.out.println("Line inside reduce method is:\n"+line);
		try{
		 if(line.length()>0 && line.split(":")[1].trim().equalsIgnoreCase("1")){
			 //	 System.out.println("Reducer output line is:"+line);
			 return line;
		 }
		}catch(Exception e){
		//	System.out.println("The exception occured for line "+line);
		//	e.printStackTrace( );
		}
		return null;
	}
}
