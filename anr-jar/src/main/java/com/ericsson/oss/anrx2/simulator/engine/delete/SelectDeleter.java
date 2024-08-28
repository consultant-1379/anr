package com.ericsson.oss.anrx2.simulator.engine.delete;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.Config;

public class SelectDeleter implements Runnable {
	private long deleteInterval;
	private long deleteDelay = 0;

	private String[] plmnId;
	
	private int relsDeleted = 0;
	private int cellsDeleted = 0;
	private int eenbDeleted = 0;
	private int nodesCompleted = 0;
	
	private AtomicBoolean exitFlag;
	
	private Random rnd = new Random();
	
	
	private final static Logger logger = Logger.getLogger(SelectDeleter.class.getName()); 
	
	/* IDLE_TARGET is the % of time that we want to be idle for */
	private final static int IDLE_TARGET = 10;
	/* DELAY_STEP is amount of time to increase/decrease deleteDelay in order to reach the IDLE_TARGET */
	private final static long DELAY_STEP = 100;
	
	public SelectDeleter(AtomicBoolean exitFlag, List<String> sims) {
		this.exitFlag = exitFlag;
	}
	
	public void run() {		
		logger.fine("Deleter.run starting");
		try {
			plmnId = Config.getInstance().getPlmnId();
			updateDeleteInterval();

			while ( exitFlag.get() == false ) {
				long startTime = System.currentTimeMillis();
				if ( deleteInterval > 0 ) {
					runDeletion();
				}

				// If we're shutting down
				if ( exitFlag.get() ) {
					break;
				}
				tuneDeleteDelay(startTime);
				waitNextCycle(startTime);					
			}
			logger.fine("Deleter.run returning");
		} catch ( Throwable t ) {
			logger.log(Level.SEVERE, "Failed to runDeletion ", t);
			System.exit(1);
		}			
	}

	private void tuneDeleteDelay(long startTime) {
		if ( deleteInterval == 0 ) {
			return;
		}
		
		long now = System.currentTimeMillis();
		long duration = now - startTime;
		if ( duration > deleteInterval ) {
			if ( deleteDelay > 0 ) {
				deleteDelay -= DELAY_STEP;
				logger.info("Reducing delete delay to " + deleteDelay);
			}
		} else {
			long idleTime = deleteInterval - duration;
			if ( ((idleTime*100)/deleteInterval) > IDLE_TARGET ) {
				deleteDelay += DELAY_STEP;
				logger.info("Idle for " + (idleTime/1000) + " secs, increasing delete delay to " + deleteDelay);
			}
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
	
	private void updateDeleteInterval() throws Exception {
		long newDeleteInterval = Long.parseLong(Config.getInstance().getManditoryParam("deleteInterval")) * 60000L;
		if ( newDeleteInterval != deleteInterval ) {
			deleteInterval = newDeleteInterval; 
			logger.fine("updateDeleteInterval: deleteInterval=" + deleteInterval);
		}
	}
	
	private void runDeletion() throws Exception {
		logger.info("runDeletion: Starting");
		long startTime = System.currentTimeMillis();

		// Read config parameters each time so we can do dynamic updates
		DeleteParams delParams = new DeleteParams();
				
		// Set to hold enbId of any node that is a candidate for deletion
		Set<Integer> enbIds = new HashSet<Integer>();

		// First look for any nodes which have auto created rels which are older then the hold time
		long deleteRelsOlderThen = System.currentTimeMillis() - delParams.relKeepTime;
		Date relDeleteDate = new Date(deleteRelsOlderThen);
		List<EUtranCellRelation> deletableRels = 
				EUtranCellRelation.getMatching("timeOfCreation < '" + TimeOfCreation.getInstance().format(relDeleteDate) + "'");
		for ( EUtranCellRelation rel : deletableRels) {
			enbIds.add(new Integer(rel.enbIdA));
		}

		long deleteEEUCOlderThen = System.currentTimeMillis() - delParams.cellKeepTime;
		Date extCellDeleteDate = new Date(deleteEEUCOlderThen);
		List<ExternalEUtranCellFDD> deletableCells = 
				ExternalEUtranCellFDD.getMatching("refCount = 0 AND lastUpdated < '" + TimeOfCreation.getInstance().format(extCellDeleteDate)  + "'");
		for ( ExternalEUtranCellFDD cell : deletableCells ) {
			enbIds.add(cell.ownerEnbId);
		}

		long deleteEENFOlderThen = System.currentTimeMillis() - delParams.funcKeepTime;
		Date extENBDeleteDate = new Date(deleteEENFOlderThen);
		List <ExternalENodeBFunction> deletableEENBF =
				ExternalENodeBFunction.getMatching("refCount = 0 AND lastUpdated < '" + TimeOfCreation.getInstance().format(extENBDeleteDate)  + "'");
		for ( ExternalENodeBFunction eenb :  deletableEENBF ) {
			enbIds.add(eenb.ownerEnbId);
		}
		logger.info("runDeletion: Found " + enbIds.size() + " nodes with MOs for deletion");

		int preRelsDeleted = relsDeleted;
		int preCellsDeleted = cellsDeleted;
		int preEENBDeleted = eenbDeleted;
		int preNodesCompleted = nodesCompleted;
		
		int deleteThreads = Integer.parseInt(Config.getInstance().getManditoryParam("deleteThreads"));
		ExecutorService es = Executors.newFixedThreadPool(deleteThreads);		
		for ( Integer enbId : enbIds ) {
			if ( BlackList.getInstance().contains(enbId) ) {
				logger.fine("Skipping blacklisted node " + enbId);
			} else {
				es.execute(new OneNode(enbId.intValue(),deleteDelay, delParams, plmnId));
			}
		}
		es.shutdown();
		
		while ( ! es.isTerminated() ) { 
			es.awaitTermination(30, TimeUnit.SECONDS);
			logger.fine("runDeletion: nodesCompleted = " + (nodesCompleted-preNodesCompleted));
		}
		
		logger.info("runDeletion: Completed in " + ((System.currentTimeMillis() - startTime)/1000) + 
				" secs, Rels " + (relsDeleted - preRelsDeleted) + 
				" Cells " + (cellsDeleted - preCellsDeleted) + 
				" EENB " + (eenbDeleted - preEENBDeleted));
	}

	private synchronized void nodeCompleted( int relsDeleted, int cellsDeleted, int eenbDeleted ) {
		this.nodesCompleted++;
		this.relsDeleted += relsDeleted;
		this.cellsDeleted += cellsDeleted;
		this.eenbDeleted += eenbDeleted;
	}
	
	class OneNode implements Runnable {
		private final int enbId;
		private final long deleteDelay;
		private DeleteNode dn;

		OneNode( int enbId, long deleteDelay, DeleteParams delParams, String[] plmnId ) {
			this.enbId = enbId;
			this.deleteDelay = deleteDelay;
			this.dn = new DeleteNode(enbId, delParams, plmnId, rnd);
		}
		
		public void run() {
			if (exitFlag.get())
				return;

			try {								
				int delCounts[] = dn.processNode();
				if ( (delCounts[0] + delCounts[1] + delCounts[2]) > 0 && deleteDelay > 0) {
					Thread.sleep(deleteDelay);
				}
				nodeCompleted( delCounts[0], delCounts[1], delCounts[2]);
			} catch ( Throwable t ) {				
				logger.log(Level.SEVERE, "Failed to process node with eNBId " + enbId, t);
				BlackList.getInstance().add(enbId);
				//System.exit(1);
			}
		}
	}
	
	
}
