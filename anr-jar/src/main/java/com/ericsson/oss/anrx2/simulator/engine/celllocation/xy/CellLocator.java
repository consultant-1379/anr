package com.ericsson.oss.anrx2.simulator.engine.celllocation.xy;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.DistanceCalculator;

public class CellLocator {
	static class Offset {
		final int x;
		final int y;
		Offset(int x, int y) {
			this.x = x;
			this.y = y;
		}
	} 
	
	private final static Logger logger = Logger.getLogger(CellLocator.class.getName());
	
	private static Offset[] NINE_CELL = new Offset[] { new Offset(2,2), new Offset(2,5), new Offset(2,8),
													   new Offset(5,2), new Offset(5,5), new Offset(5,8),
													   new Offset(8,2), new Offset(8,5), new Offset(8,8) };
	private static Offset[] ONE_CELL = new Offset[] { new Offset(5,5) };
	private static Offset[] SIX_CELL = new Offset[] { new Offset(3,2), new Offset(3,5), new Offset(3,8),
		new Offset(7,2), new Offset(7,5), new Offset(7,8) };
	
	private static Offset[] THREE_CELL = new Offset[] { new Offset(3,3), new Offset(6,3), new Offset(5,7) };
	private static Offset[] TWELVE_CELL = new Offset[] { new Offset(2,2), new Offset(2,5), new Offset(2,8),
		   new Offset(4,2), new Offset(4,5), new Offset(4,8),
		   new Offset(6,2), new Offset(6,5), new Offset(6,8),
		   new Offset(8,2), new Offset(8,5), new Offset(8,8) };
	private static Offset[] TWO_CELL = new Offset[] { new Offset(5,3), new Offset(5,7) };
	
	private final int boxSize = 20;
	private final int nodesPerRow = 12;
	private Map<Integer,Offset[]> offsets = new HashMap<Integer,Offset[]>();
	private final String simA;
	
	private final String simB;
	
	public CellLocator(String simA, String simB) {
		this.simA = simA;
		this.simB = simB;
		
		offsets.put(ONE_CELL.length, ONE_CELL);
		offsets.put(TWO_CELL.length, TWO_CELL);
		offsets.put(THREE_CELL.length, THREE_CELL);
		offsets.put(SIX_CELL.length, SIX_CELL);
		offsets.put(NINE_CELL.length, NINE_CELL);
		offsets.put(TWELVE_CELL.length, TWELVE_CELL);		
	}
	
	public boolean assign() throws Exception {
		boolean updateRequiredA = assign(simA, 0, 0);
		boolean updateRequiredB = assign(simB, 10, 0);
		 
		return updateRequiredA || updateRequiredB;
	}

	private boolean assign(String sim, int xOffset, int yOffset) throws Exception {
		logger.fine("assign sim " + sim + " xOffset " + xOffset + " yOffset " + yOffset);
		List<EUtranCellFDD> cellsInSim = EUtranCellFDD.getMatching("fdn LIKE '%,SubNetwork=" + sim + ",%' ORDER BY eNBId, cellId");
		logger.fine("assign found " + cellsInSim.size() + " cells");
		Map<Integer,List<EUtranCellFDD>> cellsByNode = new HashMap<Integer,List<EUtranCellFDD>>();
		for ( EUtranCellFDD cell : cellsInSim ) {
			List<EUtranCellFDD> cellsInNode = cellsByNode.get(cell.enbId);
			if ( cellsInNode == null ) {
				cellsInNode = new LinkedList<EUtranCellFDD>();
				cellsByNode.put(cell.enbId, cellsInNode);
			}
			cellsInNode.add(cell);
		}
		List<Integer> enbIds = new LinkedList<Integer>(cellsByNode.keySet());
		logger.fine("assign found " + enbIds.size() + " enbIds");
		
		Collections.sort(enbIds);
		int nodeIndex = 0;
		List<DbChange> dbChanges = new LinkedList<DbChange>();
		for ( Integer enbId : enbIds ) {
			List<EUtranCellFDD> cellsInNode = cellsByNode.get(enbId);			
			int x = ((nodeIndex % nodesPerRow) * boxSize) + xOffset;
			int y = ((nodeIndex / nodesPerRow) * boxSize) + yOffset;
			logger.fine("assign enbId=" + enbId + " num Cells=" + cellsInNode.size() + " x=" + x + " y=" + y);

			Offset cellOffsets[] = offsets.get(cellsInNode.size());
			if ( cellOffsets == null ) {
				throw new IllegalStateException("No cellOffsets for " + enbId + ", num cells = " + cellsInNode.size());
			}
			int cellIndex = 0;
			for ( EUtranCellFDD cell : cellsInNode ) {
				int cellX = x + cellOffsets[cellIndex].x;
				int cellY = y + cellOffsets[cellIndex].y;
				if ( cell.location.latitude != cellX || cell.location.longitude != cellY ) {
					cell.location = new CellLocation(cellY,cellX,0);
					dbChanges.add(new DbChange(ChangeType.UPDATE,cell));
				}
				cellIndex++;
			}
			nodeIndex++;
		}
		
		boolean updateRequired = false;
		if ( dbChanges.size() > 0 ) {
			Db.getInstance().loadChanges(dbChanges);
			updateRequired = true;
		}
		
		return updateRequired;
	}
	
	public void assignRelDist() throws Exception {
		logger.fine("assignRelDist simA=" + simA + " simB=" + simB);		
		Map<CellIdentity,EUtranCellFDD> cellMap = new HashMap<CellIdentity,EUtranCellFDD>();
		for ( String sim : new String[] { simA, simB } ) {
			List<EUtranCellFDD> cellsInSim = EUtranCellFDD.getMatching("fdn LIKE '%,SubNetwork=" + sim + ",%' ORDER BY eNBId, cellId");
			for ( EUtranCellFDD cell : cellsInSim ) {			
				cellMap.put(new CellIdentity(cell.enbId,cell.cellId), cell);
			}
		}
		
		List<DbChange> dbUpdates = new LinkedList<DbChange>();
		
		for ( EUtranCellFDD from : cellMap.values() ) {
			List<EUtranCellRelation> rels = EUtranCellRelation.getMatching("enbIdA = " + from.enbId + " AND cellIdA = " + from.cellId );
			for ( EUtranCellRelation rel : rels ) {
				EUtranCellFDD to = cellMap.get(new CellIdentity(rel.enbIdB,rel.cellIdB));
				if ( to == null ) {
					throw new IllegalStateException("Cannot find to cell for rel " + rel);
				}
				int dist = DistanceCalculator.distance(from.location,to.location);
				if ( rel.dist != dist ) {
					rel.dist = dist;
					dbUpdates.add(new DbChange(ChangeType.UPDATE,rel));
				}
			}
		}
		logger.fine("assignRelDist updating " + dbUpdates.size() + " EUtranCellRelation");
		if ( dbUpdates.size() > 0 ) {
			Db.getInstance().loadChanges(dbUpdates);
		}
	}
		
	public Map<CellIdentity,Map<CellIdentity,Integer>> calDistance() throws Exception {
		logger.fine("calDistance simA=" + simA + " simB=" + simB);		
		Map<CellIdentity,Map<CellIdentity,Integer>> result = new HashMap<CellIdentity,Map<CellIdentity,Integer>>();
		List<EUtranCellFDD> cellsInSimA = EUtranCellFDD.getMatching("fdn LIKE '%,SubNetwork=" + simA + ",%' ORDER BY eNBId, cellId");
		List<EUtranCellFDD> cellsInSimB = EUtranCellFDD.getMatching("fdn LIKE '%,SubNetwork=" + simB + ",%' ORDER BY eNBId, cellId");
		logger.fine("calDistance " + cellsInSimA.size() + " in " + simA + " AND " + cellsInSimB.size() + " in " + simB);
		
		for ( EUtranCellFDD from : cellsInSimA ) {
			CellIdentity fromCID = new CellIdentity(from.enbId,from.cellId);
			Map <CellIdentity,Integer> distFromCell = new HashMap<CellIdentity,Integer>();
			result.put(fromCID, distFromCell);
			for ( EUtranCellFDD to : cellsInSimB ) {				
				distFromCell.put(new CellIdentity(to.enbId,to.cellId), DistanceCalculator.distance(from.location, to.location));
			}
		}
		
		return result;
	}
}
