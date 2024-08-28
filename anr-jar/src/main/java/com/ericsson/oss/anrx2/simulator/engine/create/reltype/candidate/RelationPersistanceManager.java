package com.ericsson.oss.anrx2.simulator.engine.create.reltype.candidate;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.Config;



public class RelationPersistanceManager implements Runnable {
	List<Integer> enbIds = new LinkedList<Integer>();
	private final AtomicBoolean exitFlag;
	private Random rnd = new Random();
	private long interval;
	private final static Logger logger = Logger.getLogger(RelationPersistanceManager.class.getName()); 
	
	public RelationPersistanceManager(AtomicBoolean exitFlag, List<String> sims) throws Exception {
		this.exitFlag = exitFlag;
		
		for ( String sim : sims ) {
		//	List<ENodeBFunction> enbInSim = ENodeBFunction.getMatching("fdn LIKE '%,MeContext=%" + sim + "%' ORDER BY eNBId");
		//	for ENM sometimes fdn is starting with MeContext, so removing comma from the filter to satisfy all cases
			List<ENodeBFunction> enbInSim = ENodeBFunction.getMatching("fdn LIKE '%MeContext=%" + sim + "%' ORDER BY eNBId");
			for ( ENodeBFunction enb : enbInSim ) {
				enbIds.add(enb.eNBId);
			}
		}
		Collections.sort(enbIds);		
	}
	
	public void run() {
		Iterator<Integer> itr = enbIds.iterator();
		
		try {			
			int Threads = Integer.parseInt(Config.getInstance().getManditoryParam("candidatePersistenceThreads"));
			ExecutorService es = Executors.newFixedThreadPool(Threads);		
			
			updateInterval();
			
			while ( exitFlag.get() == false ) {
				long thisCycleStart = System.currentTimeMillis();
				
				if ( interval > 0 ) {
					if ( ! itr.hasNext() ) {
						itr = enbIds.iterator();
					}
					Integer enbId = itr.next();
					if ( BlackList.getInstance().contains(enbId) ) {
						logger.fine("run: Skipping blacklisted node " + enbId);
					} else {
						es.submit(new CandidatePersistenceRunner(enbId, rnd));
					}
				}
				
				waitNextCycle(thisCycleStart);
			}
		} catch ( Exception e ) {
			logger.log(Level.SEVERE, "Persistance failed", e);
			System.exit(1);
		}
	}
	
	private void waitNextCycle(long thisCycleStart) throws Exception {
		if ( logger.isLoggable(Level.FINEST) ) 
			logger.finest("waitNextCycle thisCycleStart=" + new Date(thisCycleStart) + 
				", Interval=" + interval);
		long nextCycleStart = Long.MAX_VALUE; // Default to wait forever (disabled)

		long now = 0;
		do {
			now = System.currentTimeMillis();
			updateInterval();
			if ( interval > 0 ) {
				nextCycleStart = thisCycleStart + interval;
			} else {
				nextCycleStart = Long.MAX_VALUE;
			}

			if ( logger.isLoggable(Level.FINEST) ) 
				logger.finest("waitNextCycle now=" + now + ", nextCycleStart=" + nextCycleStart + 
					", Interval=" + interval);

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
	
	private void updateInterval() throws Exception {
		long newInterval = Long.parseLong(Config.getInstance().getManditoryParam("candidatePersistanceInterval"));
		if ( newInterval != interval ) {
			interval = newInterval; 
			logger.fine("updateInterval: Interval=" + interval);
		}
	}
	
}
