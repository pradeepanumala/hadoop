package mapper;

import taskTracker.TaskTrackerUtility;

public class Mapper implements IMapper {
	/*public void testm(){
		System.out.println("Inside testm");
	}*/

	@Override
	public String map(String line) {
		//System.out.println("the line in mapper####################### "+line);
		String word=TaskTrackerUtility.grepWord;
		if(line.contains(word)){
			return line+":1";
		}
		return line+":0";
	}

}
