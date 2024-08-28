package com.ericsson.oss.anrx2.simulator.engine.delete.reltype.persist;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFreqRelation;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.IdFactory;
import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteParams;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.IRelationDeleter;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;

public class PersistentRelationDeleter implements IRelationDeleter {
	private final static Logger logger = Logger.getLogger(PersistentRelationDeleter.class.getName()); 

	@Override
	public int delete(SimConnection simConn, Timestamp lastUpdated, 
			int enbId, String meId,
			String[] plmnId, Random rnd,
			boolean cleanNode, DeleteParams delParams)
			throws Exception {
		List<DbChange> dbChanges = new LinkedList<DbChange>();
		KertayleSession ks = new KertayleSession();

		// EUtranCellRelation
		long deleteRelsOlderThen = lastUpdated.getTime() - delParams.relKeepTime;
		Date relDeleteDate = new Date(deleteRelsOlderThen);
		List<EUtranCellRelation> deletableRels = 
				EUtranCellRelation.getMatching("enbIdA = " + enbId + " AND timeOfCreation <= '" + TimeOfCreation.getInstance().format(relDeleteDate) + "'");

		if ( deletableRels.size() == 0 ) {
			return 0;
		}
		

		// Hold the list of ExternalEUtranCellFDD that the relations point at
		// Note: We may be removing relations which point at the same ExternalEUtranCellFDD
		//       so we need to cache the ExternalEUtranCellFDD to ensure that we update the
		//       refCount correctly
		Map<String,ExternalEUtranCellFDD> updatedCells = new HashMap<String,ExternalEUtranCellFDD>();
		int totalMidRels = 0;
		int deletedMidRels = 0;
		int totalFarRels = 0;		
		int deletedFarRels = 0;
		for ( EUtranCellRelation rel : deletableRels ) {
			boolean doDelete = true;
			if ( rel.dist < delParams.nearDist ) {
				// Relation is to a "nearby" cell so we'll never delete
				if ( logger.isLoggable(Level.FINEST)) 
					logger.finest("deleteRelations: Skipping near " + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB + " dist=" + rel.dist);
				doDelete = false;
			} else if ( rel.dist < delParams.farDist ) {
				totalMidRels++;
				// Relation is to a "middle" cell so we delete it according to the relThreshold
				if ( rnd.nextInt(1000) > delParams.relThreshold) {
					logger.fine("deleteRelations: Skipping mid " + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB + " dist=" + rel.dist);
					doDelete = false;
				} else {
					deletedMidRels++;
				}
			} else {
				// Relation is to a far away cell so we always delete it
				doDelete = true;
				totalFarRels++;				
				deletedFarRels++;
			}			
			if ( ! doDelete ) {
				continue;
			}
			
			dbChanges.add(new DbChange(ChangeType.DELETE,rel));				

			EUtranCellFDD containingCell = EUtranCellFDD.getMatching("enbId = " + enbId + " AND cellId = " + rel.cellIdA ).get(0);
			EUtranFreqRelation containingFreqRel = EUtranFreqRelation.getMatching("enbId = " + enbId +" AND cellId = " + rel.cellIdA + " AND arfcnValueEUtranDl = " + rel.arfcnValueEUtranDl).get(0);
			String relFdn = containingCell.fdn + ",EUtranFreqRelation=" + containingFreqRel.rdnId + ",EUtranCellRelation=" + IdFactory.cellId(plmnId,rel.enbIdB,rel.cellIdB); 
			ks.deleteMO(relFdn);

			// Now get the corresponding ExternalEUtranCellFDD and decrement it's refCount
			String cellKey = String.valueOf(rel.enbIdB) + "-" + String.valueOf(rel.cellIdB);
			ExternalEUtranCellFDD relCell = updatedCells.get(cellKey);
			if ( relCell == null ) {
				List<ExternalEUtranCellFDD> eeucList = 
						ExternalEUtranCellFDD.getMatching("ownerId = " + enbId + " AND targetId = " + rel.enbIdB + " AND localCellId = " + rel.cellIdB);
				if ( eeucList.size() == 1 ) {
					relCell = eeucList.get(0);
					updatedCells.put(cellKey, relCell);
				} else {
					logger.warning("deleteRelations: Could not find target ExternalEUtranCellFDD for " + 
									rel.enbIdA + "-" + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB);
				}
			}

			if ( relCell != null ) {
				relCell.lastUpdated = lastUpdated;
			}
		}
		
		int relsDeleted = 0;
		if( (deletedMidRels + deletedFarRels) > 0 ) {
			logger.info(meId + ": Deleting " + (deletedMidRels+deletedFarRels) + 
					" EUtranCellRelations (" + deletedMidRels +"/" + deletedFarRels + " of " +
					totalMidRels + "/" + totalFarRels + ")");
			for ( ExternalEUtranCellFDD relCell : updatedCells.values() ) {
				dbChanges.add(new DbChange(ChangeType.UPDATE,relCell));
			}

			simConn.execKertayle(meId, ks, false);					
			Db.getInstance().loadChanges(dbChanges);				
			dbChanges.clear();
			
			relsDeleted = (deletedMidRels + deletedFarRels);			
		}
		
		return relsDeleted;
	}

}
