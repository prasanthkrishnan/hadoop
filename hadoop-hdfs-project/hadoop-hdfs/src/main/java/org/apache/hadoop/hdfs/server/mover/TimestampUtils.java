import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

package org.apache.hadoop.hdfs.server.mover;

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
			Timestamp timestamp = new Timestamp(secondsToMilli(seconds));
			toReturn.add(timestamp);	
		}
		return toReturn;	
	}	
	
	// Return true if the timestamp falls within the last 24hrs
	public static boolean isWithinDay(Timestamp timestamp, long currentTime){
		return isWithinDuration(timestamp, currentTime, MILLIS_PER_DAY);
	}	
	
	// Return true if the timestamp falls within the last Week
	public static boolean isWithinWeek(Timestamp timestamp, long currentTime){
		return isWithinDuration(timestamp, currentTime, MILLIS_PER_WEEK);
	}	
	
	// Return true if the timestamp falls within the last Month
	public static boolean isWithinMonth(Timestamp timestamp, long currentTime){
		return isWithinDuration(timestamp, currentTime, MILLIS_PER_MONTH);
	}	
	
	private static long secondsToMilli(long seconds){
		return TimeUnit.SECONDS.toMillis(seconds);
	}
	
	private static long milliToSeconds(long milliseconds){
		return TimeUnit.MILLISECONDS.toSeconds(milliseconds);
	}

	// currentTime is in seconds and duration is in milliseconds
	private static boolean isWithinDuration(Timestamp timestamp, long currentTime, long duration){
		Timestamp current = new Timestamp(secondsToMilli(currentTime));
		Timestamp lastDay = new Timestamp(secondsToMilli(currentTime) - duration);	
		if (timestamp.before(current) && timestamp.after(lastDay)){
			return true;
		}
		else{
			return false;
		}	
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
		List<Timestamp> stamps = convertToTimeStamps("3434345,2356453432,1430712858,1430629140,1430110756,1430283556,1428124664,14282974644343");
		for(Timestamp t: stamps){
			System.out.println(t.toString());
		}
	
		long currentTime = milliToSeconds(System.currentTimeMillis());
		System.out.println(isWithinDay(stamps.get(0), currentTime));
		System.out.println(isWithinDay(stamps.get(1), currentTime));
		System.out.println(isWithinDay(stamps.get(2), currentTime));
		System.out.println(isWithinDay(stamps.get(3), currentTime));
		System.out.println(isWithinWeek(stamps.get(4), currentTime));
		System.out.println(isWithinWeek(stamps.get(5), currentTime));
		System.out.println(isWithinDay(stamps.get(5), currentTime));
		System.out.println(isWithinMonth(stamps.get(4), currentTime));
		System.out.println(isWithinMonth(stamps.get(5), currentTime));
		System.out.println(isWithinMonth(stamps.get(6), currentTime));
		System.out.println(isWithinMonth(stamps.get(7), currentTime));
		
	}*/
	
}
