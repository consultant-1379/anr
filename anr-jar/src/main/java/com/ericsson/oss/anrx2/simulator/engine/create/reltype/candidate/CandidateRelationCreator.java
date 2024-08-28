package com.ericsson.oss.anrx2.simulator.engine.create.reltype.candidate;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.CandNeighborRel;
import com.ericsson.oss.anrx2.simulator.db.CandidateRelatedCellData;
import com.ericsson.oss.anrx2.simulator.db.ChangeType;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.DbChange;
import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFreqRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFrequency;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.CreatedByEutran;
import com.ericsson.oss.anrx2.simulator.engine.FdnHelper;
import com.ericsson.oss.anrx2.simulator.engine.IdFactory;
import com.ericsson.oss.anrx2.simulator.engine.LastModificationEutran;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.DistanceCalculator;
import com.ericsson.oss.anrx2.simulator.engine.create.CreateData;
import com.ericsson.oss.anrx2.simulator.engine.create.NodeData;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.RelationCreatorFactory;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnectionFactory;
import com.ericsson.oss.anrx2.simulator.netsim.candidate.CandRelHelper;

public class CandidateRelationCreator implements IRelationCreator {
	private final static Logger logger = Logger.getLogger(CandidateRelationCreator.class.getName()); 

	protected final String[] plmnId;
	
	public CandidateRelationCreator() throws Exception {
		plmnId = Config.getInstance().getPlmnId();				
	}

	@Override
	public Set<CellIdentity> getRelatedCells(CellIdentity ci) throws Exception {
		logger.finer("getRelatedCells");

		Set<CellIdentity> results = new HashSet<CellIdentity>();
		for ( ICellRelation rel  : RelationCreatorFactory.getInstance().getRelations("enbIdA = " + ci.enbId + " AND cellIdA =  " + ci.cellId) ) {
			results.add(rel.getCellB());
		}		
		
		return results;
	}

	@Override
	public boolean isRelated(CellIdentity cia, CellIdentity cib, CreateData cd)
			throws Exception {
		logger.finer("isRelated");
		
		String filterFmt = "(enbIdA = %d AND cellIdA = %d AND enbIdB = %d AND cellIdB = %d)";
		String filter = String.format(filterFmt, cia.enbId, cia.cellId, cib.enbId, cib.cellId) + " OR " +
				String.format(filterFmt, cib.enbId, cib.cellId, cia.enbId, cia.cellId);
		List<EUtranCellRelation> existingRels = EUtranCellRelation.getMatching(filter);
		boolean result = true;
		if ( existingRels.size() > 0 ) {
			result = true;
		} else {
			List<CandNeighborRel> existingCandRels = CandNeighborRel.getMatching(filter);
			if ( existingCandRels.size() > 0 ) {
				result = true;
			} else {
				result = false;
			}
		}
		
		logger.finer("isRelated: " + cia + "->" + cib + " " + result);
		return result;
	}

	protected void createProxiesAndRel(NodeData src, NodeData targ, int dist,
			boolean createEENB, 
			List<DbChange> dbUpdates, KertayleSession kSess) throws Exception {
		logger.finer("createProxiesAndRel");
					
	}
	
	@Override
	public void createRelation(CreateData cd) throws Exception {
		logger.finer("createRelation");				
		
		SimConnection srcSimConn = null; 
		try {		
			NodeData src = cd.src;
			NodeData targ = cd.targ;
			
			srcSimConn = SimConnectionFactory.getInstance().getConnection(src.enb.sim);

			List<DbChange> dbUpdates = new LinkedList<DbChange>();		
			KertayleSession kSess = new KertayleSession();

			Date now = new Date();

			// Create ExternalENodeBFunction if required
			boolean createdEENB = false;
			String srcEENBfdn = src.enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + IdFactory.nodeId(plmnId,targ.enb.eNBId);
			if ( src.eenb == null ) {
				logger.fine("Creating ExternalENodeBFunction " + srcEENBfdn);
				src.eenb = new ExternalENodeBFunction(src.enb.eNBId, targ.enb.eNBId, CreatedByEutran.ANR, now);
				dbUpdates.add(new DbChange(ChangeType.CREATE,src.eenb));
				kSess.createExternalENodeBFunction(srcEENBfdn,src.eenb,plmnId);
				createdEENB = true;
			} 

			// Now figure out if we've to create the ExternalEUtranCellFDD
			if ( src.eeuc == null ) {
				String srcEEUCfdn = srcEENBfdn + ",ExternalEUtranCellFDD=" + IdFactory.cellId(plmnId,targ.enb.eNBId, targ.cell.cellId);
				logger.fine("Creating ExternalEUtranCellFDD " + srcEEUCfdn);
				src.eeuc = new ExternalEUtranCellFDD(src.enb.eNBId,targ.enb.eNBId,targ.cell.cellId, CreatedByEutran.ANR, now);
				dbUpdates.add(new DbChange(ChangeType.CREATE,src.eeuc));					
				kSess.createExternalEUtranCellFDD(srcEEUCfdn, src.eeuc, src.cell, src.freq.fdn, plmnId, LastModificationEutran.ANR_MODIFICATION );
				
				
				// Now if we're not creating the ExternalENodBeFuction we need to 
				// update the timestamp on the existing one
				if ( ! createdEENB ) {
					cd.src.eenb.lastUpdated = new java.sql.Timestamp(now.getTime());
					dbUpdates.add(new DbChange(ChangeType.UPDATE,cd.src.eenb));
				}				
			} else {
				// We're not creating the ExternalEUtranCellFDD but we need to update the
				// timestamp on the existing now
				cd.src.eeuc.lastUpdated = new java.sql.Timestamp(now.getTime());;
				dbUpdates.add(new DbChange(ChangeType.UPDATE,cd.src.eeuc));				
			}

			
			CandNeighborRel newCRel = new CandNeighborRel(src.enb.eNBId,src.cell.cellId,targ.enb.eNBId,targ.cell.cellId,
					src.freq.arfcnValueEUtranDl, cd.dist, now);
			dbUpdates.add(new DbChange(ChangeType.CREATE, newCRel));
			
			String srcFreqRelFdn = src.cell.fdn + ",EUtranFreqRelation=" + src.freqRel.rdnId;
			List<CandidateRelatedCellData> cRelsData = 
					CandidateRelatedCellData.getCandidateRelated(src.enb.eNBId,src.cell.cellId,src.freq.arfcnValueEUtranDl);
			CandidateRelatedCellData newCRelData = new CandidateRelatedCellData(targ.enb.eNBId,targ.cell.cellId,
					targ.cell.physicalLayerCellIdGroup,targ.cell.physicalLayerSubCellId,targ.cell.tac);		
			cRelsData.add(newCRelData);
			CandRelHelper.setCandRel(srcFreqRelFdn, cRelsData, now, plmnId, kSess,src.enb.neMIMVersion);
			
			srcSimConn.execKertayle(cd.src.meId,kSess);
			Db.getInstance().loadChanges(dbUpdates); dbUpdates.clear();

			SimConnectionFactory.getInstance().returnConnection(srcSimConn, true);
			srcSimConn = null;
		} catch ( Exception e ) {
			logger.log(Level.SEVERE, "Failed to create relation between " + FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + 
					" and " + FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD"), e);
				BlackList.getInstance().add(cd.src.enb.eNBId);
				BlackList.getInstance().add(cd.targ.enb.eNBId);			
		} finally {
			// If we're failed then blacklist both nodes			
			if ( srcSimConn != null ) {
				SimConnectionFactory.getInstance().returnConnection(srcSimConn, false);
			}
		}
		
	}

	@Override
	public boolean setup(CreateData cd) throws Exception {
		getEnbAndCells(cd.src);
		getEnbAndCells(cd.targ);
		cd.dist = DistanceCalculator.distance(cd.src.cell.location,cd.targ.cell.location);
		logger.fine("setup Trying cells " + FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + " and " +
				 FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD") + " dist " + cd.dist);
		
		// Check the PCI values
		if ( (cd.src.cell.physicalLayerCellIdGroup == cd.targ.cell.physicalLayerCellIdGroup) &&
		     (cd.src.cell.physicalLayerSubCellId == cd.targ.cell.physicalLayerSubCellId) ) {
			logger.fine("Rejecting due to ident PCI");
			return false;
		}
		
		// Now pick the frequency to use
		cd.src.freqRel = getFrequencyRel(cd.src.cell,cd.targ.cell);
		if ( cd.src.freqRel == null  ) {
			logger.fine("Could not find EUtranFreqRelation for cells " +  FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + " and " +
					FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD"));
			return false;
		}

		cd.src.freq = 
				EUtranFrequency.getMatching("enbId = " + cd.src.enb.eNBId + " AND arfcnValueEUtranDl = " + cd.src.freqRel.arfcnValueEUtranDl).get(0);
				
		
		List<ExternalENodeBFunction> srcEENBList = 
				ExternalENodeBFunction.getMatching("ownerId = " + cd.src.enb.eNBId + " AND targetId = " + cd.targ.enb.eNBId);
		logger.finer("srcEENBList.size=" + srcEENBList.size());		
		if ( srcEENBList.size() == 1  ) {
			cd.src.eenb = srcEENBList.get(0);
		} 		

		// Look for the ExternalEUtranCellFDD
		if ( cd.src.eenb != null ) {
			String filter = "ownerId = " + cd.src.enb.eNBId + " AND targetId = " + cd.targ.enb.eNBId + " AND localCellId = " + cd.targ.cell.cellId;
			List<ExternalEUtranCellFDD> matches = ExternalEUtranCellFDD.getMatching(filter);
			if ( matches.size() == 1 ) {
				cd.src.eeuc = matches.get(0);
			}
		}
		
		return true;
	}
	
	private void getEnbAndCells(NodeData nd) throws Exception {
		nd.enb = ENodeBFunction.getMatching("eNBId = " + nd.cellIdent.enbId).get(0);
		if ( nd.enb == null ) {
			throw new IllegalStateException("Could not find ENodeBFunction with id " + nd.cellIdent.enbId);
		}
		nd.meId = FdnHelper.getMoId(nd.enb.fdn, "MeContext");
		
		nd.cells = EUtranCellFDD.getMatching("enbId = " + nd.cellIdent.enbId);
		for ( EUtranCellFDD cell : nd.cells ) {
			 if ( cell.cellId == nd.cellIdent.cellId ) {
				 nd.cell = cell;
			 }
		}
		if ( nd.cell == null ) {
			throw new IllegalStateException("Could not find EUtranCellFDD with cellId " + nd.cellIdent.cellId + " for " + nd.meId);
		}
	}

	/*
	 * Get the EUtranFreqRelation under the srcCell that points at the Freq used by the targCell
	 */
	private EUtranFreqRelation getFrequencyRel(EUtranCellFDD srcCell, EUtranCellFDD targCell) throws Exception {
		List<EUtranFreqRelation> srcFreqList = 
				EUtranFreqRelation.getMatching("enbId = " + srcCell.enbId + " AND cellId = " + srcCell.cellId  + " AND arfcnValueEUtranDl = " + targCell.earfcndl);
		if ( srcFreqList.size() == 0 ) {
			return null;
		} else {
			return srcFreqList.get(0);
		}
	}
	
}
