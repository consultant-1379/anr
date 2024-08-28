package com.ericsson.oss.anrx2.simulator.engine.delete;

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

public class SequentialDeleter implements Runnable {
	class DeleteNodeRunner implements Runnable {
		final int enbId;
		final DeleteNode nd;
		
		DeleteNodeRunner(int enbId, DeleteNode nd) {
			this.enbId = enbId;
			this.nd = nd;
		}
		public void run() {
			try {
				nd.processNode();
			} catch ( Exception e ) {
				logger.log(Level.SEVERE, "Failed to delete for " + enbId, e);
				BlackList.getInstance().add(enbId);
			}			
		}		
	}

	private final static Logger logger = Logger.getLogger(SequentialDeleter.class.getName());
	private long deleteInterval;
	List<Integer> enbIds = new LinkedList<Integer>();
	private final AtomicBoolean exitFlag; 

	private Random rnd = new Random();
	
	public SequentialDeleter(AtomicBoolean exitFlag, List<String> sims) throws Exception {
		this.exitFlag = exitFlag;
		
		for ( String sim : sims ) {
	//		List<ENodeBFunction> enbInSim = ENodeBFunction.getMatching("fdn LIKE '%,MeContext=%" + sim + "%'");
	//		for ENM sometimes fdn is starting with MeContext, so removing comma from the filter to satisfy all cases
			List<ENodeBFunction> enbInSim = ENodeBFunction.getMatching("fdn LIKE '%MeContext=%" + sim + "%'");
			for ( ENodeBFunction enb : enbInSim ) {
				enbIds.add(enb.eNBId);
			}
		}
		Collections.sort(enbIds);
	}
	
	public void run() {
		Iterator<Integer> itr = enbIds.iterator();
		
		try {			
			int deleteThreads = Integer.parseInt(Config.getInstance().getManditoryParam("deleteThreads"));
			ExecutorService es = Executors.newFixedThreadPool(deleteThreads);
			
			updateDeleteInterval();
			
			String[] plmnId = Config.getInstance().getPlmnId();
			while ( exitFlag.get() == false ) {
				long thisCycleStart = System.currentTimeMillis();
				
				if ( deleteInterval > 0 ) {
					if ( ! itr.hasNext() ) {
						itr = enbIds.iterator();
					}

					Integer enbId = itr.next();
					if ( BlackList.getInstance().contains(enbId) ) {

						logger.fine("run: Skipping blacklisted node " + enbId);
					} else {
						DeleteParams delParams = new DeleteParams();
						DeleteNode delNode = new DeleteNode(enbId, delParams, plmnId, rnd);
						es.submit(new DeleteNodeRunner(enbId, delNode));
					}
				}
				waitNextCycle(thisCycleStart);
			}
		} catch ( Exception e ) {
			logger.log(Level.SEVERE, "Deletion failed", e);
			System.exit(1);
		}
	}
	
	private void updateDeleteInterval() throws Exception {
		long newDeleteInterval = Long.parseLong(Config.getInstance().getManditoryParam("deleteInterval"));
		if ( newDeleteInterval != deleteInterval ) {
			deleteInterval = newDeleteInterval; 
			logger.fine("updateDeleteInterval: deleteInterval=" + deleteInterval);
		}
	}
	
	
	private void waitNextCycle(long thisCycleStart) throws Exception {
		if ( logger.isLoggable(Level.FINEST) ) 
			logger.finest("waitNextCycle thisCycleStart=" + new Date(thisCycleStart) + 
				", deleteInterval=" + deleteInterval);
		long nextCycleStart = Long.MAX_VALUE; // Default to wait forever (deletion disabled)

		long now = 0;
		do {
			now = System.currentTimeMillis();
			updateDeleteInterval();
			if ( deleteInterval > 0 ) {
				nextCycleStart = thisCycleStart + deleteInterval;
			} else {
				nextCycleStart = Long.MAX_VALUE;
			}

			if ( logger.isLoggable(Level.FINEST) ) 
				logger.finest("waitNextCycle now=" + now + ", nextCycleStart=" + nextCycleStart + 
					", deleteInterval=" + deleteInterval);

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
}
