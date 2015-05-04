import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TimestampUtils{

	public final static long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;
	public final static long MILLIS_PER_WEEK = 24 * 60 * 60 * 1000L * 7;
	public final static long MILLIS_PER_MONTH = 24 * 60 * 60 * 1000L * 30;
	
	//Assume input in seconds
	public static List<Timestamp> convertToTimeStamps(String epochString){	
		List<Timestamp> toReturn = new ArrayList<Timestamp>();
		String[] splitString = epochString.split(",");
		for(String s: splitString){
			long seconds = Long.parseLong(s);
			long milliseconds = TimeUnit.SECONDS.toMillis(seconds);
			Timestamp timestamp = new Timestamp(milliseconds);
			toReturn.add(timestamp);	
		}
		return toReturn;	
	}	
	
	// Return true if the timestamp falls within the last 24hrs
	public static boolean isWithinDuration(Timestamp timestamp, long duration){
		Timestamp current = new Timestamp(System.currentTimeMillis());
		Timestamp lastDay = new Timestamp(System.currentTimeMillis() - duration);	
		if (timestamp.before(current) && timestamp.after(lastDay)){
			return true;
		}
		else{
			return false;
		}	
	}	
	
	// Return true if the timestamp falls within the last 24hrs
	public static boolean isWithinDay(Timestamp timestamp){
		return isWithinDuration(timestamp, MILLIS_PER_DAY);
	}	
	
	// Return true if the timestamp falls within the last 24hrs
	public static boolean isWithinWeek(Timestamp timestamp){
		return isWithinDuration(timestamp, MILLIS_PER_WEEK);
	}	
	
	// Return true if the timestamp falls within the last 24hrs
	public static boolean isWithinMonth(Timestamp timestamp){
		return isWithinDuration(timestamp, MILLIS_PER_MONTH);
	}	
	
	/*
	public static List<Timestamp> convertToTimeStamps(String epochString){	
		List<Timestamp> toReturn = new ArrayList<Timestamp>();
		String[] splitString = epochString.split(",");
		for(String s: splitString){
			long lTimestamp = Long.parseLong(s);
			Timestamp timestamp = new Timestamp(lTimestamp);
			toReturn.add(timestamp);	
		}
		return toReturn;	
	}	
	*/

	/*
	public static void main(String[] args){
		List<Timestamp> stamps = convertToTimeStamps("3434345,2356453432,1430712858,1430629140,1430110756,1430283556,1428124664,1428297464");
		for(Timestamp t: stamps){
			System.out.println(t.toString());
		}
		System.out.println(isWithinDay(stamps.get(0)));
		System.out.println(isWithinDay(stamps.get(1)));
		System.out.println(isWithinDay(stamps.get(2)));
		System.out.println(isWithinDay(stamps.get(3)));
		System.out.println(isWithinWeek(stamps.get(4)));
		System.out.println(isWithinWeek(stamps.get(5)));
		System.out.println(isWithinDay(stamps.get(5)));
		System.out.println(isWithinMonth(stamps.get(4)));
		System.out.println(isWithinMonth(stamps.get(5)));
		System.out.println(isWithinMonth(stamps.get(4)));
		System.out.println(isWithinMonth(stamps.get(6)));
		System.out.println(isWithinMonth(stamps.get(7)));
	}*/
	
}
