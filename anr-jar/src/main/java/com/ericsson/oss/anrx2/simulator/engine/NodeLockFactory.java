package com.ericsson.oss.anrx2.simulator.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NodeLockFactory {
	private static NodeLockFactory inst;
	private Map<Integer, Lock> nodeLocks = new HashMap<Integer,Lock>();
	
	public static synchronized NodeLockFactory getInstance() {
		if ( inst == null ) {
			inst = new NodeLockFactory();
		}
		return inst;
	}
		
	public synchronized Lock getLock(int enbId) {
		Lock lock = nodeLocks.get(enbId);
		if ( lock == null ) {
			lock = new ReentrantLock();
			nodeLocks.put(enbId,lock);
		}
		return lock;
	}
}
