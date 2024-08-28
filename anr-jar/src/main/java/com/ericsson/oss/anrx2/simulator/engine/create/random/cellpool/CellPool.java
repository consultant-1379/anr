package com.ericsson.oss.anrx2.simulator.engine.create.random.cellpool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.FdnHelper;
import com.ericsson.oss.anrx2.simulator.engine.NodeLockFactory;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.DistanceCalculator;
import com.ericsson.oss.anrx2.simulator.engine.create.CellDistance;
import com.ericsson.oss.anrx2.simulator.engine.create.CreateData;
import com.ericsson.oss.anrx2.simulator.engine.create.DistanceRanges;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.RelationCreatorFactory;

public class CellPool implements IRandomRelationCreator {
	private final static Logger logger = Logger.getLogger(CellPool.class.getName());

	private Random rnd = new Random();
	
	private final Map<CellIdentity,DistanceRanges> distFromCellA = new HashMap<CellIdentity,DistanceRanges>();
	private final List<CellIdentity> cellIds;
	
	public CellPool(String simList[]) throws Exception {
		Map<CellIdentity,CellLocation> cells = new HashMap<CellIdentity,CellLocation>();
		for ( String sim : simList ) {
		//	List<EUtranCellFDD> cellsInSim = EUtranCellFDD.getMatching("fdn LIKE '%,MeContext=%" + sim + "ERBS%'");
		//	for ENM sometimes fdn is starting with MeContext, so removing comma from the filter to satisfy all cases
			List<EUtranCellFDD> cellsInSim = EUtranCellFDD.getMatching("fdn LIKE '%MeContext=%" + sim + "%ERBS%'");
			for ( EUtranCellFDD cell : cellsInSim ) {
				cells.put(new CellIdentity(cell.enbId,cell.cellId),cell.location);
			}
		}
		
		int near = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.nearDist"));
		int far = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.farDist"));
		int max = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.maxDist"));
		
		cellIds = new ArrayList<CellIdentity>(cells.keySet());
		for ( CellIdentity srcId :  cellIds ) {
			DistanceRanges distRanges = new DistanceRanges();			
			logger.finer("CellPool(): calculating distances from " + srcId + "(" + cells.get(srcId) + ")");
			for ( CellIdentity destId :  cellIds ) {
				if ( srcId.enbId != destId.enbId ) { // Exclude cells on same node
					int distance = DistanceCalculator.distance(cells.get(srcId), cells.get(destId));
					if ( logger.isLoggable(Level.FINEST) ) logger.finest("CellPool(): distance to " + destId + "(" + cells.get(destId) + ") = " + distance);
					if ( distance < max ) {
						CellDistance cd = new CellDistance(destId,new Integer(distance));
						
						if ( distance < near ) {
							distRanges.near.add(cd);
						} else if ( cd.dist < far ) {
							distRanges.mid.add(cd);
						} else if ( cd.dist < max ) {
							distRanges.far.add(cd);
						}
					}
				}
			}
			logger.fine("CellPool(): " + srcId + " near " + distRanges.near.size() + " mid " + distRanges.mid.size() + " far " + distRanges.far.size());
			// We want near cells sorted in distance order
			Collections.sort(distRanges.near);
			
			// Mid and far cells should be in a random order
			// We will "consume" them in sequence
			Collections.shuffle(distRanges.mid);
			Collections.shuffle(distRanges.far);
			
			// Now figure out which relations are already in use
			List<DbChange> dbUpdates = new LinkedList<DbChange>();
			List<ICellRelation> existingRels = RelationCreatorFactory.getInstance().getRelations("enbIdA = " + srcId.enbId + " AND cellIdA = " + srcId.cellId);
			for ( ICellRelation rel : existingRels ) {
				int distance = rel.getDistance();
				if ( distance == 0 ) {
					CellLocation targetCellLoc = cells.get(rel.getCellB());
					if ( targetCellLoc == null ) {
						throw new IllegalStateException("Invalid relation in " + srcId + ", targetCell not found " + rel.getCellB());
					}
					distance = DistanceCalculator.distance(cells.get(srcId), targetCellLoc);
					rel.setDistance(distance);
					dbUpdates.add(new DbChange(ChangeType.UPDATE,rel));
				}
				
				if ( distance < near ) {
					distRanges.nearUsed++;
				} else {
					// If the relation is in the range list, remove it and add it the end of the list
					CellDistance cd = new CellDistance(rel.getCellB(), distance);
					List<CellDistance> cellsInRange = distRanges.mid;
					if ( distance >= far ) {
						cellsInRange = distRanges.far;
					}
					if ( cellsInRange.remove(cd) ) {
						cellsInRange.add(cd);
					}
				}
			}			
			distFromCellA.put(srcId, distRanges);
			if ( dbUpdates.size() > 0 ) {
				Db.getInstance().loadChanges(dbUpdates);
			}
		}
	}
	
	public void createRelation() throws Exception {
		logger.info("createRandomRelation: Started");

		long startTime = System.currentTimeMillis();
		
		// Select the cells to use		
		CreateData cd = null;
		int retryCount = 0;
		IRelationCreator rc = RelationCreatorFactory.getInstance().makeRelationCreator();
		boolean selectSuccess = false;
		while ( selectSuccess == false ) { 
			try {
				cd = new CreateData();
				if ( selectPair(cd,rc) ) {
					selectSuccess = rc.setup(cd);
				}
			} catch ( Exception e ) {
				logger.log(Level.FINE, "selectPair " + retryCount + " failed",e);
			}
			if ( selectSuccess == false ) {
				if ( cd.lockNodeA != null ) {
					cd.lockNodeA.unlock();
				}
				if ( cd.lockNodeB != null ) {
					cd.lockNodeB.unlock();
				}
				
				retryCount++;
				if ( retryCount > 10 ) {
					logger.warning("Failed to select pair");
					return;
				}
			}				
		}	
			
		logger.info("createRandomRelation: " + FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + "->" +
				 FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD") + " dist " + cd.dist + " x2 " + cd.x2SetupRequired);

		try {
			rc.createRelation(cd);
		} finally {
			cd.lockNodeA.unlock();
			cd.lockNodeB.unlock();			
		}
		
		long endTime = System.currentTimeMillis();
		logger.info("createRandomRelation: Completed in " + (endTime-startTime) +  " ms");		
	}

	private boolean selectPair(CreateData cd, IRelationCreator rc) throws Exception {
		CellIdentity cia = cellIds.get(rnd.nextInt(cellIds.size()));
		Lock lockA = NodeLockFactory.getInstance().getLock(cia.enbId);
		if ( lockA.tryLock() == false ) {
			logger.fine("selectPair failed as node already locked " + cia.enbId);
			return false;
		}
		cd.lockNodeA = lockA;
				
		DistanceRanges distRanges = distFromCellA.get(cia);
		if ( distRanges == null ) {
			throw new IllegalStateException("No distList found for " + cia);			
		}

		// First check if there's any near cell that we're not related to
		// Optimization here: Near relations are never deleted, so we "remember"
		// which ones we used using the CellDistance.nearUsed
		logger.fine("selectPair " + cia + " nearUsed=" + distRanges.nearUsed);
		CellIdentity cib = null;
		for ( ; distRanges.nearUsed < distRanges.near.size() && cib == null ; distRanges.nearUsed++ ) {
			CellDistance cellDist = distRanges.near.get(distRanges.nearUsed);
			if (  ! checkRelated(rc,cia,cellDist.cid,cd) ) {
				cib = cellDist.cid;
			}
		}
		// Okay, we already related to all nearby cells
		
		// Optimization here: it's very expensive to check in the db if we are related to a
		// cell or it's related to us, especially when the mid/far ranges are nearly full
		// So we use one query to get all the cells that this cell is related to (existingRels)
		// When checking if we can create a rel to a cell, we check the if it's in existingRels
		// If not, then we use isRelated to double check that the target cell is not related to us and
		// lock the target node
		Set<CellIdentity> existingRels = rc.getRelatedCells(cia);		
		if ( cib == null ) {
			logger.fine("selectPair near range full for " + cia);
			// Now decide we what to relate to a mid or far cell
			List<CellDistance> cellsInRange = distRanges.mid;
			int midFarRatio = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.midFarRatio"));
			if ( rnd.nextInt(100) >= midFarRatio ) {
				logger.fine("selectPair Using far range for " + cia);
				cellsInRange = distRanges.far;
			}
						
			// We take the first cell in the range out of the list and
			// add it back to the end of the list. This means that it should 
			// be likely that the cells at the front of list don't have a relation
			// i.e. by the time we cycle through the list, the odds are that the 
			// relation will be deleted
			for ( int tryCount = 0; tryCount < cellsInRange.size() && cib == null; tryCount++ ) {
				CellDistance cellDist = cellsInRange.remove(0);
				cellsInRange.add(cellDist);				
				if ( !existingRels.contains(cellDist.cid) && ! checkRelated(rc,cia,cellDist.cid,cd)) {
					cib = cellDist.cid;
				}
			}
		}					
		
		
		if (  cib == null ) {
			if ( logger.isLoggable(Level.FINER) ) logger.finer("selectPair failed as we could find any cells not related to " + cia);
			return false;
		}
		
		if ( BlackList.getInstance().contains(cia.enbId) ) {
			if ( logger.isLoggable(Level.FINER) ) logger.finer("selectPair failed as " + cia.enbId + " blacklisted");
			return false;			
		}
		if ( BlackList.getInstance().contains(cib.enbId) ) {
			if ( logger.isLoggable(Level.FINER) ) logger.finer("selectPair failed as " + cib.enbId + " blacklisted");
			return false;			
		}

		
		cd.src.cellIdent = cia; 
		cd.targ.cellIdent = cib;

		return true;
	}
	
	private boolean checkRelated(IRelationCreator rc, CellIdentity cia, CellIdentity cib, CreateData cd) throws Exception {
		cd.lockNodeB = NodeLockFactory.getInstance().getLock(cib.enbId);
		if ( ! cd.lockNodeB.tryLock() ) {
			cd.lockNodeB = null;
			throw new Exception("Node already locked: " + cib.enbId); 
		}
		
		if ( rc.isRelated(cia,cib,cd) ) {
			cd.lockNodeB.unlock();
			cd.lockNodeB = null;
			return true;
		} else {
			return false;
		}		
	}
	
}
