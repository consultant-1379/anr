package com.ericsson.oss.anrx2.simulator.engine.delete;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.db.TermPointToENB;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.NodeLockFactory;
import com.ericsson.oss.anrx2.simulator.engine.TwinKey;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.RelationCreatorFactory;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.IRelationDeleter;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.RelationDeleterFactory;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnectionFactory;

public class DeleteNode {
	private final int enbId;
	private final Random rnd;
	private final DeleteParams delParams;
	private final String[] plmnId;
	
	int relsDeleted = 0;
	int cellsDeleted = 0;
	int eenbDeleted = 0;

	private final static Logger logger = Logger.getLogger(DeleteNode.class.getName()); 

	public DeleteNode(int enbId, DeleteParams delParams, String[] plmnId, Random rnd) {
		this.enbId = enbId;
		this.delParams = delParams;
		this.plmnId = plmnId;
		this.rnd = rnd;
	}

	public int[] processNode() throws Exception {
		ENodeBFunction enb = ENodeBFunction.getMatching("eNBId = " + enbId ).get(0);
		String meConFdn = enb.fdn.substring(0,enb.fdn.indexOf(",ManagedElement"));
		String meId = meConFdn.substring(meConFdn.lastIndexOf("=")+1);
		logger.info(meId + ": Started processing");
		
		long now = System.currentTimeMillis();
		Timestamp lastUpdated = new Timestamp(now);
		
		Lock nodeLock = NodeLockFactory.getInstance().getLock(enbId);
		nodeLock.lock();
		
		SimConnection simConn = null;
		try {
			String simlation = enb.sim;
			simConn = SimConnectionFactory.getInstance().getConnection(simlation);
			
			boolean cleanNode = delParams.cellKeepTime == 0 && delParams.relKeepTime == 0;
			IRelationDeleter relDeleter = RelationDeleterFactory.getInstance().makeRelationDeleter();
			relDeleter.delete(simConn, lastUpdated, enbId, meId, plmnId, rnd, cleanNode, delParams);
			
			// Get the list of remain relations			
			List<ICellRelation> relations = RelationCreatorFactory.getInstance().getRelations("enbIdA = " + enb.eNBId);
			
			Set<Integer> usedEnbIds = deleteExtCells(simConn, lastUpdated, meId, enb, relations);
			deleteEENB(simConn, lastUpdated, meId, enb, usedEnbIds);
						
			SimConnectionFactory.getInstance().returnConnection(simConn,true);
			simConn = null;
		} finally {
			nodeLock.unlock();

			if ( simConn != null ) {
				SimConnectionFactory.getInstance().returnConnection(simConn,false);
			}
		}			
		logger.info(meId + ": Completed processing");
		
		return new int[] { relsDeleted, cellsDeleted, eenbDeleted };
	}
		
	private Set<Integer> deleteExtCells(SimConnection simConn, Timestamp lastUpdated, String meId, ENodeBFunction enb, List<ICellRelation> relations) throws Exception {
		logger.fine("deleteExtCells");
		List<DbChange> dbChanges = new LinkedList<DbChange>();
		KertayleSession ks = new KertayleSession();

		Set<Integer> usedEnbIds = new HashSet<Integer>();
		
		/*		  
		 * From Automated Neighbor Relations 27/1553-HSC 105 50/1-V1 Uen Y   
		 *  2.3.5   Removal of Unused Neighbor Objects
		 *  After that the ExternalEUtranCellFDD or ExternalEUtranCellTDD are removed. Here we have two situations
		 *   with TermPointToENB enabled or no TermPointToENB disabled or locked. 
		 *   For the case where we don't have any X2 connection between the nodes, then ExternalEUtranCellFDD 
		 *   and ExternalEUtranCellTDD are removed one by one if not used for removeNcellTime: in this case it is 
		 *   considered to be used if there are EUtranCellRelation configured that points to this MO (relations 
		 *   configured to any of the internal cells)
		 *   For the case when TermPointToENB is enabled: then all ExternalEUtranCellFDD and ExternalEUtranCellTDD 
		 *   must be ready to remove, no EUtranCellRelation for removeNcellTime. In this case first 
		 *   TermPointToENB is removed, then the cells ExternalEUtranCellFDD and ExternalEUtranCellTDD.
		 *   
		 *  From X2 Configuration 61/1553-HSC 105 50/1-V1 Uen J
		 *  2.3.4   Removal of X2 Connection by Locking
		 *   X2 removal is initiated by locking the TermPointToENB MO in the RBS. The MO is locked either 
		 *   manually or by the Automated Neighbor Relation feature. The RBS shuts down (or aborts) the SCTP 
		 *   connection. The target RBS sets TermPointToENB to LOCKED.
         *   Note: All MOs created at X2 setup remain in the both RBSs.
		 */
				
		
		// Group the ExternalEUtranCellFDD by targetId
		List<ExternalEUtranCellFDD> eeucList = ExternalEUtranCellFDD.getMatching("ownerId = " + enbId);
		if ( eeucList.size() == 0 ) {
			// No ExternalEUtranCellFDD so nothing to do
			return usedEnbIds;
		}
		
		Map<Integer, List<ExternalEUtranCellFDD>> eeucByTargId = new HashMap<Integer,List<ExternalEUtranCellFDD>>();
		for ( ExternalEUtranCellFDD eeuc : eeucList ) {
			List<ExternalEUtranCellFDD> eeucForTarg = eeucByTargId.get(eeuc.targetEnbId);
			if ( eeucForTarg == null ) {
				eeucForTarg = new LinkedList<ExternalEUtranCellFDD>();
				eeucByTargId.put(eeuc.targetEnbId,eeucForTarg);
			}
			eeucForTarg.add(eeuc);
		}

		// Build a list of what external cells are in use, i.e. reservedBy a relation
		Set<TwinKey> referencedCells = new HashSet<TwinKey>();
		for ( ICellRelation rel : relations ) {
			referencedCells.add(new TwinKey(rel.getCellB().enbId,rel.getCellB().cellId));
		}

		// Check if all ExternalEUtranCellFDD for each target can be deleted
		long deleteEEUCOlderThen = lastUpdated.getTime() - delParams.cellKeepTime;
		List<ExternalEUtranCellFDD> deletableCells = new LinkedList<ExternalEUtranCellFDD>();
		List<TermPointToENB> deleteTPTENB = new LinkedList<TermPointToENB>();
		for ( Map.Entry<Integer,List<ExternalEUtranCellFDD>> eeucForTargId : eeucByTargId.entrySet() ) {
			Integer targEnbId = eeucForTargId.getKey();
			List<ExternalEUtranCellFDD> eeucForTarg = eeucForTargId.getValue();
			boolean canDelete = true;
			int targetId = 0;			
			for ( ExternalEUtranCellFDD eeuc : eeucForTarg ) {
				targetId = eeuc.targetEnbId;
				if ( referencedCells.contains(new TwinKey(eeuc.targetEnbId,eeuc.localCellId)) || 
						eeuc.lastUpdated.getTime() > deleteEEUCOlderThen ) {
					canDelete = false;
				}
			}
			if ( canDelete ) {
				deletableCells.addAll(eeucForTarg);
				
				List<TermPointToENB> matches = TermPointToENB.getMatching("ownerId = " + enb.eNBId + " AND targetId =" + targetId);
				if ( matches.size() == 1 ) {
					deleteTPTENB.add(matches.get(0));
				}
			} else {
				usedEnbIds.add(targEnbId);
			}
		}

		if ( deletableCells.size() ==  0 ) {
			return usedEnbIds;
		}
		
		logger.info(meId + ": Deleting " + deletableCells.size() + " ExternalEUtranCellFDDs");
		logger.info(meId + ": Deleting " + deleteTPTENB.size() + " TermPointToENBs");
		
		// Hold the list of ExternalENodeBFunction that the contain the ExternalEUtranCellFDD
		Map<Integer,ExternalENodeBFunction> updatedEENB = new HashMap<Integer,ExternalENodeBFunction>();		
		for ( ExternalEUtranCellFDD cell : deletableCells ) {
			dbChanges.add(new DbChange(ChangeType.DELETE,cell));

			String eenbId = plmnId[0] + plmnId[1] + "-" + cell.targetEnbId;
			String eeucId = eenbId + "-" + cell.localCellId;
			String eeucFdn = enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + eenbId + ",ExternalEUtranCellFDD=" + eeucId;
			ks.deleteMO(eeucFdn);
			
			ExternalENodeBFunction eenb = updatedEENB.get(new Integer(cell.targetEnbId));
			if ( eenb == null ) {
				eenb =
						ExternalENodeBFunction.getMatching("ownerId = " + enbId + " AND targetId = " + cell.targetEnbId).get(0);
				updatedEENB.put(cell.targetEnbId, eenb);
			}
			eenb.lastUpdated = lastUpdated;
		}
		for ( ExternalENodeBFunction eenb : updatedEENB.values()) {
			dbChanges.add(new DbChange(ChangeType.UPDATE,eenb));
		}
		
		for ( TermPointToENB tptENB : deleteTPTENB) {
			String eenbId = plmnId[0] + plmnId[1] + "-" + tptENB.targetEnbId;
			String tptENB_Fdn = enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + eenbId + ",TermPointToENB=" + eenbId;
			ks.deleteMO(tptENB_Fdn);
			dbChanges.add(new DbChange(ChangeType.DELETE,tptENB));
		}
		
		simConn.execKertayle(meId, ks, false);
		Db.getInstance().loadChanges(dbChanges);
		dbChanges.clear();
		
		cellsDeleted = deletableCells.size();
		
		return usedEnbIds;
	}
	
	private void deleteEENB(SimConnection simConn, Timestamp lastUpdated, String meId, ENodeBFunction enb, Set<Integer> referencedEENB) throws Exception {
		logger.fine("deleteEENB");

		// ExternalENodeBFunction 
		long deleteEENFOlderThen = System.currentTimeMillis() - delParams.funcKeepTime;
		Date extENBDeleteDate = new Date(deleteEENFOlderThen);
		List <ExternalENodeBFunction> possibleEENBF =
				ExternalENodeBFunction.getMatching("ownerId = " + enbId + " AND lastUpdated <= '" + TimeOfCreation.getInstance().format(extENBDeleteDate)  + "'");
		if ( possibleEENBF.size() == 0 ) {
			return;
		}

		int deletingEENB = 0;
		int deletingtptENB = 0;
		
		List<DbChange> dbChanges = new LinkedList<DbChange>();
		KertayleSession ks = new KertayleSession();		
		for ( ExternalENodeBFunction eenb :  possibleEENBF ) {
			if ( ! referencedEENB.contains(eenb.targetEnbId) ) {
				deletingEENB++;
				String eenbId = plmnId[0] + plmnId[1] + "-" + eenb.targetEnbId;
				String eenbFdn = enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + eenbId; 				
				
				// Normally the TermPointToENB should have been deleted when the ExternalEUtranCellFDDs
				// were deleted but just check here to be sure
				List<TermPointToENB> matches = TermPointToENB.getMatching("ownerId = " + eenb.ownerEnbId+ " AND targetId =" + eenb.targetEnbId);
				if ( matches.size() > 0 ) {
					deletingtptENB++;
					TermPointToENB tptENB = matches.get(0);				
					dbChanges.add(new DbChange(ChangeType.DELETE,tptENB));
					String tptENB_Fdn =  eenbFdn + ",TermPointToENB=" + eenbId;
					ks.deleteMO(tptENB_Fdn);
				}
				
				dbChanges.add(new DbChange(ChangeType.DELETE,eenb));
				ks.deleteMO(eenbFdn);								
			}
		}
		if ( deletingtptENB > 0 ) {
			logger.info(meId + ": Deleting " + deletingtptENB + " TermPointToENBs");			
		}		
		if ( deletingEENB > 0 ) { 
			logger.info(meId + ": Deleting " + deletingEENB + " ExternalENodeBFunctions");
		}
		if ( dbChanges.size() > 0 ) {
			simConn.execKertayle(meId, ks, false);
			Db.getInstance().loadChanges(dbChanges);
			dbChanges.clear();
		}
		
		eenbDeleted = deletingEENB;		
	}
}
