package com.ericsson.oss.anrx2.simulator.db;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeOfCreation {
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static TimeOfCreation inst;
	
	public static synchronized TimeOfCreation getInstance() {
		if ( inst == null ) {
			inst = new TimeOfCreation();
		}
		return inst;
	}
	
	public synchronized String format(Date date) {
		return sdf.format(date);
	}
	
	public synchronized Date parse(String timestamp) throws ParseException {
		return sdf.parse(timestamp);
	}
}
