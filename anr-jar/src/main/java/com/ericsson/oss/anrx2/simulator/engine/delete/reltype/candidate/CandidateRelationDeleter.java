package com.ericsson.oss.anrx2.simulator.engine.delete.reltype.candidate;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.CandNeighborRel;
import com.ericsson.oss.anrx2.simulator.db.CandidateRelatedCellData;
import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranFreqRelation;
import com.ericsson.oss.anrx2.simulator.engine.TwinKey;
import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteParams;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.IRelationDeleter;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.persist.PersistentRelationDeleter;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;
import com.ericsson.oss.anrx2.simulator.netsim.candidate.CandRelHelper;

public class CandidateRelationDeleter implements IRelationDeleter {
	private final static Logger logger = Logger.getLogger(CandidateRelationDeleter.class.getName()); 

	@Override
	public int delete(SimConnection simConn, Timestamp lastUpdated, int enbId,			
			String meId, String[] plmnId, Random rnd, boolean cleanNode,		
			DeleteParams delParams) throws Exception {
		logger.fine("delete: cleanNode=" + cleanNode);
		
		if ( cleanNode ) {
			deleteAllCandidateRels(simConn, meId, enbId, plmnId);
			PersistentRelationDeleter persistRelDeleter = 
					new PersistentRelationDeleter();
			return persistRelDeleter.delete(simConn, lastUpdated, enbId, meId, plmnId, rnd, cleanNode, delParams);
		} else {
			return 0;
		}
	}

	private void deleteAllCandidateRels(SimConnection simConn, String meId,
			int enbId, String[] plmnId) throws Exception {
		List<DbChange> dbChanges = new LinkedList<DbChange>();
		KertayleSession ks = new KertayleSession();
		ENodeBFunction enb = ENodeBFunction.getMatching("eNBId = " + enbId ).get(0);
		Set<TwinKey> freqRelIds = new HashSet<TwinKey>();
		for ( CandNeighborRel cRel : CandNeighborRel.getMatching("enbIdA = " + enbId)) {
			dbChanges.add(new DbChange(ChangeType.DELETE,cRel));
			freqRelIds.add(new TwinKey(cRel.cellIdA,cRel.arfcnValueEUtranDl));
		}
		
		if ( freqRelIds.size() == 0 ) {
			return;
		}
		
		Map <Integer,EUtranCellFDD> allCells = new HashMap<Integer,EUtranCellFDD>();
		for ( EUtranCellFDD cell : EUtranCellFDD.getMatching("enbId = " + enbId) ) {
			allCells.put(cell.cellId, cell);
		}		
		Map<TwinKey,EUtranFreqRelation> allFreqRel = new HashMap<TwinKey,EUtranFreqRelation>();
		for ( EUtranFreqRelation freqRel : EUtranFreqRelation.getMatching("enbId = " + enbId) ) {
			allFreqRel.put(new TwinKey(freqRel.cellId,freqRel.arfcnValueEUtranDl), freqRel);
		}
		
		List<CandidateRelatedCellData> empty = new LinkedList<CandidateRelatedCellData>();
		Date now = new Date();
		for ( TwinKey freqRelId : freqRelIds ) {
			logger.finer("deleteAllCandidateRels processing freqRelId " + freqRelId);
			EUtranCellFDD cell = allCells.get(freqRelId.a);
			EUtranFreqRelation freqRel = allFreqRel.get(freqRelId);
			logger.finer("deleteAllCandidateRels cell=" + cell);
			logger.finer("deleteAllCandidateRels freqRel=" + freqRel);			
			String srcFreqRelFdn = allCells.get(freqRelId.a).fdn + ",EUtranFreqRelation=" +
					allFreqRel.get(freqRelId).rdnId;

			//for 16A support adding mimversion to check
			CandRelHelper.setCandRel(srcFreqRelFdn, empty, now, plmnId, ks,enb.neMIMVersion);
		}

		logger.info(meId + ": Deleting " + dbChanges.size() + " candidates in " + " " + freqRelIds.size() + " EUtranFreqRelations");
		
		simConn.execKertayle(meId, ks, false);
		Db.getInstance().loadChanges(dbChanges);		
		
	}
	
}
