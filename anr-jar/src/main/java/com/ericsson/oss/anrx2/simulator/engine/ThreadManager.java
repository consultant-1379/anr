package com.ericsson.oss.anrx2.simulator.engine;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadManager {
	private static ThreadManager instance;
	private final AtomicBoolean exitFlag = new AtomicBoolean(false);
	private final List<Thread> waitThreads = new LinkedList<Thread>();
	
	public static synchronized ThreadManager getInstance() {
		if ( instance == null ) {
			instance = new ThreadManager();
		}
		
		return instance;
	}

	public AtomicBoolean getExitFlag() {
		return exitFlag;
	}

	public void registerThread(Thread thread) {
		waitThreads.add(thread);
	}
	
	public void shutdown() throws InterruptedException {
        exitFlag.set(true); 
        synchronized(exitFlag){
            exitFlag.notifyAll();
        }
		for ( Thread thread : waitThreads ) {
			thread.join();
		}
	}
}
