package com.ericsson.oss.anrx2.simulator.engine.create.random.pairsims;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.OperExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.FdnHelper;
import com.ericsson.oss.anrx2.simulator.engine.NodeLockFactory;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.xy.CellLocator;
import com.ericsson.oss.anrx2.simulator.engine.create.CellDistance;
import com.ericsson.oss.anrx2.simulator.engine.create.CreateData;
import com.ericsson.oss.anrx2.simulator.engine.create.DistanceRanges;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.RelationCreatorFactory;

public class PairedSims {
	private final static Logger logger = Logger.getLogger(PairedSims.class.getName());
	
	private List<CellIdentity> cellIdsSimA;
	private List<CellIdentity> cellIdsSimB;

	private Map<CellIdentity,DistanceRanges> distFromCellA = new HashMap<CellIdentity,DistanceRanges>();
	
	private Random rnd = new Random();
	
	public final String simA;	
	public final String simB; 
	

	public PairedSims(String simA, String simB) throws Exception {
		this.simA = simA;
		this.simB = simB;
		
		cellIdsSimA = loadCellIds(simA);
		cellIdsSimB = loadCellIds(simB);
		
		verifyNoOperatorEENB();

		CellLocator cellLocator = new CellLocator(simA,simB);
		boolean updatedLocs = cellLocator.assign();
		if ( updatedLocs ) {
			cellLocator.assignRelDist();
		}
		processDistances(cellLocator.calDistance());
	}
		
	public void createRandomRelation() throws Exception {
		logger.info("createRandomRelation: " + simA + ":" + simB + " Started");

		long startTime = System.currentTimeMillis();
		
		// Select the cells to use		
		CreateData cd = null;
		boolean srcA = false;
		int retryCount = 0;

		IRelationCreator rc = RelationCreatorFactory.getInstance().makeRelationCreator();
		
		boolean selectSuccess = false;
		while ( selectSuccess == false ) { 
			srcA = rnd.nextBoolean();
			try {
				cd = new CreateData();
				if ( selectPair(srcA,cd,rc) ) {
					selectSuccess = rc.setup(cd);
				}
			} catch ( Exception e ) {
				logger.log(Level.FINE, "selectPair " + retryCount + " failed for " + simA + ":" + simB,e);
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
					logger.warning("Failed to select pair for " + simA + ":" + simB);
					return;
				}
			}				
		}	
			
		logger.info("createRandomRelation: " + FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + "->" +
				 FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD") + " dist " + cd.dist + 
				 " x2 " + cd.x2SetupRequired);

		try {
			rc.createRelation(cd);
		} finally {
			cd.lockNodeA.unlock();
			cd.lockNodeB.unlock();
		}
		
		long endTime = System.currentTimeMillis();
		logger.info("createRandomRelation: " + simA + ":" + simB + " Completed in " + (endTime-startTime) +  " ms");		
	}


	private int[] getMinMax( List<CellIdentity> cellIds ) { 
		int range[] = { -1, -1 };
		for ( CellIdentity cid : cellIds ) {
			if ( range[0] == -1 || cid.enbId < range[0]) {
				range[0] = cid.enbId;
			}
			if ( range[1] == -1 || cid.enbId > range[1] ) {
				range[1] = cid.enbId;
			}
		}
		
		return range;
	}
		
	private List<CellIdentity> loadCellIds(String sim) throws Exception {
		List<EUtranCellFDD> cellsInSim = EUtranCellFDD.getMatching("fdn LIKE '%,SubNetwork=" + sim + ",%'");
		List<CellIdentity> cellIds = new ArrayList<CellIdentity>(cellsInSim.size());
		for ( EUtranCellFDD cell : cellsInSim ) {
			cellIds.add(new CellIdentity(cell.enbId,cell.cellId));
		}
		return cellIds;
	}


	private void processDistances(Map<CellIdentity, Map<CellIdentity, Integer>> distMap) {
		int near = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.nearDist"));
		int far = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.farDist"));
		int max = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.maxDist"));
		
		for ( Map.Entry<CellIdentity,Map<CellIdentity,Integer>> fromEntry : distMap.entrySet() ) {
			DistanceRanges distRanges = new DistanceRanges();			
			for ( Map.Entry<CellIdentity, Integer> toEntry : fromEntry.getValue().entrySet() ) {
				CellDistance cd = new CellDistance(toEntry.getKey(),toEntry.getValue()); 
								
				if ( cd.dist < near ) {
					distRanges.near.add(cd);
				} else if ( cd.dist < far ) {
					distRanges.mid.add(cd);
				} else if ( cd.dist < max ) {
					distRanges.far.add(cd);
				}
			}
			Collections.sort(distRanges.near);
			Collections.sort(distRanges.mid);
			Collections.sort(distRanges.far);
			distFromCellA.put(fromEntry.getKey(), distRanges);			
		}		
	}

	private boolean selectPair(boolean srcA, CreateData cd, IRelationCreator rc) throws Exception {
		CellIdentity cia = cellIdsSimA.get(rnd.nextInt(cellIdsSimA.size()));
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
		if ( cib == null ) {
			logger.fine("selectPair near range full for " + cia);
			// Now decide we what to relate to a mid or far cell
			List<CellDistance> cellsInRange = distRanges.mid;
			int midFarRatio = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.midFarRatio"));
			if ( rnd.nextInt(100) >= midFarRatio ) {
				logger.fine("selectPair Using far range for " + cia);
				cellsInRange = distRanges.far;
			}
			
			int offset = rnd.nextInt(cellsInRange.size());
			for ( int tryCount = 0; tryCount < cellsInRange.size() && cib == null; tryCount++ ) {
				CellDistance cellDist = cellsInRange.get((tryCount+offset)%cellsInRange.size());
				if ( ! checkRelated(rc,cia,cellDist.cid,cd)) {
					cib = cellDist.cid;
				}
			}
		}					
		
		
		if (  cib == null ) {
			if ( logger.isLoggable(Level.FINER) ) logger.fine("selectPair failed as we could find any cells not related to " + cia);
			return false;
		}
		
		if ( BlackList.getInstance().contains(cia.enbId) ) {
			if ( logger.isLoggable(Level.FINER) ) logger.fine("selectPair failed as " + cia.enbId + " blacklisted");
			return false;			
		}
		if ( BlackList.getInstance().contains(cib.enbId) ) {
			if ( logger.isLoggable(Level.FINER) ) logger.fine("selectPair failed as " + cib.enbId + " blacklisted");
			return false;			
		}

		
		if ( srcA ) {
			cd.src.cellIdent = cia; 
			cd.targ.cellIdent = cib;
		} else {
			cd.targ.cellIdent = cia;
			cd.src.cellIdent = cib;			
		}
		
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
	
	private void verifyNoOperatorEENB() throws Exception {
		int rangeA[] = getMinMax(cellIdsSimA);
		int rangeB[] = getMinMax(cellIdsSimB);
		
		List<OperExternalENodeBFunction> operCreatedEENB_A2B = 
				OperExternalENodeBFunction.getMatching("(ownerId BETWEEN " + rangeA[0] + " AND " + rangeA[1] + ") AND " + 
		"(targetId BETWEEN " + rangeB[0] + " AND " + rangeB[1] + ")");
		if ( operCreatedEENB_A2B.size() > 0 ) {
			throw new IllegalStateException("ERROR: Found " + operCreatedEENB_A2B.size() + " operator created ExternalENodeBFunction relating " + simA + ":" + simB);
		}

		List<OperExternalENodeBFunction> operCreatedEENB_B2A = 
				OperExternalENodeBFunction.getMatching("(ownerId BETWEEN " + rangeB[0] + " AND " + rangeB[1] + ") AND " + 
		"(targetId BETWEEN " + rangeA[0] + " AND " + rangeA[1] + ")");
		if ( operCreatedEENB_B2A.size() > 0 ) {
			throw new IllegalStateException("ERROR: Found " + operCreatedEENB_B2A.size() + " operator created ExternalENodeBFunction relating " + simB + ":" + simA);
		}
	}
	
//	private EUtranCellFDD[] getSourceAndTargetCells(int srcENBId, int targENBId,
//			List<EUtranCellFDD> cellsInSrc, List<EUtranCellFDD> cellsInTarg) throws Exception {
//		final int srcStartIndex = rnd.nextInt(cellsInSrc.size());
//		final int targStartIndex = rnd.nextInt(cellsInTarg.size());
//		
//		String filter = "(enbIdA = " + srcENBId + " AND enbIdB = " + targENBId + 
//				") OR ( enbIdA = " + targENBId + " AND enbIdB = " + srcENBId + ")";
//		List<EUtranCellRelation> existingRels = EUtranCellRelation.getMatching(filter);
//		logger.fine("Found " + existingRels.size() + " relations");
//		Set<String> relKeys = new HashSet<String>(); {
//			for ( EUtranCellRelation rel : existingRels) {
//				relKeys.add(String.format("%d-%d-%d-%d",rel.enbIdA,rel.cellIdA,rel.enbIdB,rel.cellIdB));
//				relKeys.add(String.format("%d-%d-%d-%d",rel.enbIdB,rel.cellIdB,rel.enbIdA,rel.cellIdA));
//			}
//		}
//		for ( int srcCount = 0; srcCount < cellsInSrc.size(); srcCount++ ) {
//			EUtranCellFDD srcCell = cellsInSrc.get( (srcCount + srcStartIndex) % cellsInSrc.size() );
//			for ( int targCount = 0; targCount < cellsInTarg.size(); targCount++ ) {
//				EUtranCellFDD targCell = cellsInTarg.get( (targCount + targStartIndex) % cellsInTarg.size() );
//				// Now check if this pair of cells already have a relation
//				String relKey = String.format("%d-%d-%d-%d",srcCell.enbId,srcCell.cellId,targCell.enbId,targCell.cellId);
//				if ( ! relKeys.contains(relKey) ) {
//					return new EUtranCellFDD[] { srcCell, targCell };
//				}
//			}
//		}
//
//		return null;
//	}
//	
	
}
