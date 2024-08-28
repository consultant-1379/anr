package com.ericsson.oss.anrx2.simulator.engine.create;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;

public class CreatorTimer implements Runnable {
	private final static Logger logger = Logger.getLogger(CreatorTimer.class.getName()); 
	
	private long createInterval;
	private final AtomicBoolean exitFlag;
	private final IRandomRelationCreator relCreator;
	
	private final ExecutorService es;
	
	public CreatorTimer( AtomicBoolean exitFlag, IRandomRelationCreator relCreator ) throws Exception {
		this.exitFlag = exitFlag;
		this.relCreator = relCreator;
		
		int createThreads = Integer.parseInt(Config.getInstance().getManditoryParam("createThreads"));
		es = Executors.newFixedThreadPool(createThreads);		
	}

	public void run() {
		logger.fine("Creator.run starting");
		try {
			updateCreateInterval();

			while ( exitFlag.get() == false ) {
				long startTime = System.currentTimeMillis();

				if ( createInterval > 0 ) {
					es.execute(new RunOneCreate());
				}
				waitNextCycle(startTime);
			}				
		} catch ( Exception e ) {
			logger.log(Level.SEVERE, "Create failed",e);
			System.exit(1);
		}
		
		logger.fine("Creator.run closing es");
		es.shutdown();
		try { es.awaitTermination(1, TimeUnit.HOURS); } catch (InterruptedException ignored) {}
		logger.fine("Creator.run returning");
	}
	
	private void waitNextCycle(long thisCycleStart) throws Exception {
		if ( logger.isLoggable(Level.FINEST) ) 
			logger.finest("waitNextCycle thisCycleStart=" + new Date(thisCycleStart) + 
				", createInterval=" + createInterval);
		long nextCycleStart = Long.MAX_VALUE; // Default to wait forever (deletion disabled)

		long now = 0;
		do {
			now = System.currentTimeMillis();
			updateCreateInterval();
			if ( createInterval > 0 ) {
				nextCycleStart = thisCycleStart + createInterval;
			} else {
				nextCycleStart = Long.MAX_VALUE;
			}

			if ( logger.isLoggable(Level.FINEST) ) 
				logger.finest("waitNextCycle now=" + now + ", nextCycleStart=" + nextCycleStart + 
					", createInterval=" + createInterval);

			if ( now < nextCycleStart ) {
				synchronized (exitFlag) {
					try { 
						if ( nextCycleStart == Long.MAX_VALUE ) {
							exitFlag.wait();
						} else {
							exitFlag.wait(nextCycleStart - now); 						
						}
					} catch (InterruptedException ignored) {}							
				}
			}
			now = System.currentTimeMillis();				
		} while ( (exitFlag.get() != true) && (now < nextCycleStart) );
		
		if ( logger.isLoggable(Level.FINEST) ) logger.finest("waitNextCycle returning ");			
	}
	
	private void updateCreateInterval() throws Exception {
		long newCreateInterval = Long.parseLong(Config.getInstance().getManditoryParam("createInterval"));
		if ( newCreateInterval != createInterval ) {
			createInterval = newCreateInterval; 
			logger.fine("updateCreateInterval: createInterval=" + createInterval);
		}
	}
	
	class RunOneCreate implements Runnable {
		public void run() {
			try {
				if ( exitFlag.get() == true ) {
					return;
				}
				relCreator.createRelation();
			} catch ( Throwable t ) {
				logger.log(Level.SEVERE, "Failed to createRelation ", t);
				System.exit(1);
			}
		}
	}	
}		

