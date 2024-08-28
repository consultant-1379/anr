package com.ericsson.oss.anrx2.simulator.engine;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class BlackList {
	private final static Logger logger = Logger.getLogger(BlackList.class.getName()); 
	private static BlackList inst;
	private Map<Integer,Date> blackListed = new HashMap<Integer,Date>();
	public static synchronized BlackList getInstance() {
		if ( inst == null ) {
			inst = new BlackList();
		}
		return inst;
	}

	public synchronized void add(Integer enbId) {		
		blackListed.put(enbId, new Date());
		logger.warning("Adding " + enbId + ", num blacklisted nodes = " + blackListed.size());
	}

	public synchronized void remove(Integer enbId) {
		blackListed.remove(enbId);
	}

	public synchronized boolean contains(Integer enbId) {
		return blackListed.containsKey(enbId);
	}
	
	public synchronized void logList() {
		for ( Map.Entry<Integer,Date> entry : blackListed.entrySet() ) {
			logger.info("logList: " + entry.getKey() + "\t" + entry.getValue());
		}
	}
}
