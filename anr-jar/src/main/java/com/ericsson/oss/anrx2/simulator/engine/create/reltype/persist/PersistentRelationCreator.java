package com.ericsson.oss.anrx2.simulator.engine.create.reltype.persist;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import com.ericsson.oss.anrx2.simulator.db.TermPointToENB;
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
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnectionFactory;

public class PersistentRelationCreator implements IRelationCreator {
	private final static Logger logger = Logger.getLogger(PersistentRelationCreator.class.getName()); 
	
	protected final String[] plmnId;
	
	private final long x2SetupDelay;
	
	public PersistentRelationCreator() throws Exception {
		x2SetupDelay = Long.parseLong(Config.getInstance().getManditoryParam("x2SetupDelay"));	
		plmnId = Config.getInstance().getPlmnId();		
	}
	
	// Create ExternalEUtranCellFDD in the target node for each EUtranCellFDD in the src node
	// except for the cell that the relation is being created under
	protected void createOtherEECInTarg(NodeData src, NodeData targ,
			List<DbChange> dbUpdates,KertayleSession kSess) throws Exception {
		logger.finer("createOtherEECInTarg");

		Date now = new Date();

		// Get existing ExternalEUtranCellFDD in target node
		List<ExternalEUtranCellFDD> targeeucList =
				ExternalEUtranCellFDD.getMatching("ownerId = " + targ.enb.eNBId + " AND targetId = " + src.enb.eNBId);
		Map<Integer,ExternalEUtranCellFDD> targExistingProxies = new HashMap<Integer,ExternalEUtranCellFDD>();
		for ( ExternalEUtranCellFDD eeuc : targeeucList) {
			targExistingProxies.put(eeuc.localCellId,eeuc);
		}
		
		List <EUtranFrequency> targFreqList = 
				EUtranFrequency.getMatching("enbId = " + src.enb.eNBId);
		Map<Integer,EUtranFrequency> targFreqMap = new HashMap<Integer,EUtranFrequency>();
		for ( EUtranFrequency freq : targFreqList ) {
			targFreqMap.put(freq.arfcnValueEUtranDl, freq);
		}
		
		// Figure out with cells we don't have proxies for (ignoring the cell that we're currently handling)
		// i.e. targMissingCells is a list of EUtranCellFDDs in the source node that
		// we don't have ExternalEUtranCellFDDs for on the target node
		List<EUtranCellFDD> targMissingCells = new LinkedList<EUtranCellFDD>();
		for ( EUtranCellFDD euc : src.cells ) {
			if ( ! targExistingProxies.containsKey(euc.cellId) && euc.cellId != src.cell.cellId ) {
				targMissingCells.add(euc);
			}
		}
		
		String targEENBfdn = targ.enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + IdFactory.nodeId(plmnId,src.enb.eNBId);
		for ( EUtranCellFDD missingCell : targMissingCells ) {
			EUtranFrequency targFreq = targFreqMap.get(missingCell.earfcndl);
			if ( targFreq != null ) {
				String targEEUCfdn = targEENBfdn + ",ExternalEUtranCellFDD=" + 
						IdFactory.cellId(plmnId,src.enb.eNBId,missingCell.cellId);
				logger.fine("Creating ExternalEUtranCellFDD " + targEEUCfdn);
				ExternalEUtranCellFDD newProxy = new ExternalEUtranCellFDD(targ.enb.eNBId,src.enb.eNBId,missingCell.cellId, 
						CreatedByEutran.X2, now);			
				dbUpdates.add(new DbChange(ChangeType.CREATE,newProxy));
				kSess.createExternalEUtranCellFDD(targEEUCfdn, newProxy, missingCell,targFreq.fdn, 
						plmnId, LastModificationEutran.NOT_MODIFIED );				
			} else {
				logger.warning("Cannot find EUtranFrequency in " + targ.meId + " with arfcnValueEUtranDl = " +
						missingCell.earfcndl + " used by " + missingCell.fdn);
			}
		}	
	}
	
	protected void createProxiesAndRel(NodeData src, NodeData targ, int dist,
			boolean x2SetupRequired, int createdBy,
			List<DbChange> dbUpdates, KertayleSession kSess) throws Exception {
		logger.finer("createProxiesAndRel");

					
		Date now = new Date();

		// Create ExternalENodeBFunction if required
		String srcEENBfdn = src.enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + IdFactory.nodeId(plmnId,targ.enb.eNBId);
		if ( src.eenb == null ) {
			logger.fine("Creating ExternalENodeBFunction " + srcEENBfdn);
			src.eenb = new ExternalENodeBFunction(src.enb.eNBId, targ.enb.eNBId, createdBy, now);
			dbUpdates.add(new DbChange(ChangeType.CREATE,src.eenb));
			kSess.createExternalENodeBFunction(srcEENBfdn,src.eenb,plmnId);
		} else {
			src.eenb.lastUpdated = new java.sql.Timestamp(now.getTime());
		}

		createRelAndECell(src,targ,dist,x2SetupRequired,createdBy,srcEENBfdn,now,dbUpdates,kSess);
		
		if ( x2SetupRequired ) {
			String tptENBfdn =  srcEENBfdn + ",TermPointToENB=" + plmnId[0] + plmnId[1] + "-" + targ.enb.eNBId;
			dbUpdates.add(new DbChange(ChangeType.CREATE,new TermPointToENB(src.enb.eNBId,targ.enb.eNBId,CreatedByEutran.X2)));
			kSess.createTermPointToENB(tptENBfdn,now,targ.enb.ipAddress);
		}
							
	}
	
	protected void createRelAndECell(NodeData src, NodeData targ,
			int dist, boolean x2SetupRequired, int createdBy,
			String srcEENBfdn, Date now,
			List<DbChange> dbUpdates, KertayleSession kSess) throws Exception {
		logger.finer("createRelAndECell");
		
		String srcEEUCfdn = srcEENBfdn + ",ExternalEUtranCellFDD=" + IdFactory.cellId(plmnId,targ.enb.eNBId, targ.cell.cellId);
		String filter = "ownerId = " + src.enb.eNBId + " AND targetId = " + targ.enb.eNBId + " AND localCellId = " + targ.cell.cellId;
		List<ExternalEUtranCellFDD> matches = ExternalEUtranCellFDD.getMatching(filter);
		if ( matches.size() > 0 ) {
			src.eeuc = matches.get(0);
			src.eeuc.lastUpdated = new java.sql.Timestamp(now.getTime());
			dbUpdates.add(new DbChange(ChangeType.UPDATE,src.eeuc));			
		} else {
			// 	Create ExternalEUtranCellFDD 
			logger.fine("Creating ExternalEUtranCellFDD " + srcEEUCfdn);			
			src.eeuc = new ExternalEUtranCellFDD(src.enb.eNBId,targ.enb.eNBId,targ.cell.cellId, createdBy, now);
			dbUpdates.add(new DbChange(ChangeType.CREATE,src.eeuc));					
			kSess.createExternalEUtranCellFDD(srcEEUCfdn, src.eeuc, src.cell, src.freq.fdn, plmnId, LastModificationEutran.ANR_MODIFICATION );
		}

		// Create EUtranCellRelation
		String srcRelRdnId = srcEEUCfdn.substring(srcEEUCfdn.lastIndexOf("=")+1);
		String srcRelFdn = src.cell.fdn + ",EUtranFreqRelation=" + src.freqRel.rdnId +
				",EUtranCellRelation=" + srcRelRdnId; 
		logger.fine("Creating " + srcRelFdn);
		EUtranCellRelation srcEUCR = new EUtranCellRelation(src.enb.eNBId, src.cell.cellId, targ.enb.eNBId, targ.cell.cellId, 
				src.freqRel.arfcnValueEUtranDl, dist,
				createdBy, now);
		dbUpdates.add(new DbChange(ChangeType.CREATE,srcEUCR));
		kSess.createEUtranCellRelation(srcEUCR, srcRelFdn, srcEEUCfdn);
	}		

	public void createRelation(CreateData cd) throws Exception {
		logger.finer("createRelation");
		/*
		 * From X2 Configuration 61/1553-HSC 105 50/1-V1 Uen J   
		 * 2.3.3   Automatic Update of X2 Connection
		 * A configuration change in EUtranCellFDD on either side initiates the sending of an eNB configuration 
		 * update message to the remote RBS with the new cell information (Served Cells To Add, 
		 * Served Cells To Modify, and Served Cells To Delete). An eNodeB configuration update message 
		 * is also sent when an EUtranCellRelationis created on either side pointing to 
		 * a cell on the other side
		*/
				
		
		SimConnection srcSimConn = null, targSimConn = null; 
		try {		
			srcSimConn = SimConnectionFactory.getInstance().getConnection(cd.src.enb.sim);
			targSimConn = SimConnectionFactory.getInstance().getConnection(cd.targ.enb.sim);

			List<DbChange> dbUpdates = new LinkedList<DbChange>();		
			KertayleSession kSess = new KertayleSession();

			// Start of Part 1, create ExternalENodeBFunction, 	ExternalEUtranCellFDD, EUtranCellRelation & TermPointToENB
			// on src node
			createProxiesAndRel(cd.src, cd.targ, cd.dist, cd.x2SetupRequired,CreatedByEutran.ANR,
					dbUpdates,kSess);
			srcSimConn.execKertayle(cd.src.meId,kSess);
			Db.getInstance().loadChanges(dbUpdates); dbUpdates.clear();
			// End of Part 1

			// Simulate delay of X2 Setup
			Thread.sleep(x2SetupDelay);

			// Start of part 2, create ExternalENodeBFunction, 	ExternalEUtranCellFDD, EUtranCellRelation & TermPointToENB
			// on target node
			createProxiesAndRel(cd.targ, cd.src, cd.dist, cd.x2SetupRequired,CreatedByEutran.X2,
					dbUpdates,kSess);

			// Also create all the other ExternalEUtranCellFDD on targ node
			if ( cd.x2SetupRequired && cd.createAllEEUC ) {
				createOtherEECInTarg(cd.src,cd.targ,dbUpdates,kSess);
			}
			targSimConn.execKertayle(cd.targ.meId,kSess);
			Db.getInstance().loadChanges(dbUpdates); dbUpdates.clear();
			// End of Part 2


			// Simulate delay of X2 Setup
			Thread.sleep(x2SetupDelay);

			// Start of part 3, create all the other ExternalEUtranCellFDD on the src node
			if ( cd.x2SetupRequired && cd.createAllEEUC ) {
				createOtherEECInTarg(cd.targ,cd.src,dbUpdates,kSess);
				srcSimConn.execKertayle(cd.src.meId,kSess);
				Db.getInstance().loadChanges(dbUpdates); dbUpdates.clear();
			}

			// If we did an x2 setup then we need to update the timestamps 
			// as we added EEUCs during the X2 setup
			Timestamp lastUpdated = new Timestamp(System.currentTimeMillis());
			if ( cd.x2SetupRequired == true ) {
				cd.src.eenb.lastUpdated = lastUpdated;
				cd.targ.eenb.lastUpdated = lastUpdated;
				dbUpdates.add(new DbChange(ChangeType.UPDATE,cd.src.eenb));
				dbUpdates.add(new DbChange(ChangeType.UPDATE,cd.targ.eenb));				
			}

			Db.getInstance().loadChanges(dbUpdates); dbUpdates.clear();

			SimConnectionFactory.getInstance().returnConnection(srcSimConn, true);
			srcSimConn = null;
			SimConnectionFactory.getInstance().returnConnection(targSimConn, true);
			targSimConn = null;
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
			if ( targSimConn != null ) {
				SimConnectionFactory.getInstance().returnConnection(targSimConn, false);
			}			
		}
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


//	private boolean verifyEEUCCardinality(NodeData nd) throws Exception {
//		int existingAnrX2EEUC = ExternalEUtranCellFDD.getMatchingCount("ownerId = " +  nd.eenb.ownerEnbId + " AND targetId = " + nd.eenb.targetEnbId);
//		if ( existingAnrX2EEUC >= maxEEUC ) {
//			logger.fine("Cannot create ExternalEUtranCellFDD in " + nd.meId + 
//					", there are " + existingAnrX2EEUC + " ANR/X2 ExternalEUtranCellFDDs under the ExternalENodeBFunction for " + nd.eenb.targetEnbId);
//			return false;
//		} else { 
//			return true;
//		}
//	}


	public boolean setup(CreateData cd) throws Exception {
		logger.fine("setup");
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
		cd.targ.freqRel = getFrequencyRel(cd.targ.cell,cd.src.cell);
		if ( cd.src.freqRel == null || cd.targ.freqRel == null ) {
			logger.fine("Could not find EUtranFreqRelation for cells " +  FdnHelper.getMoId(cd.src.cell.fdn,"EUtranCellFDD") + " and " +
					FdnHelper.getMoId(cd.targ.cell.fdn,"EUtranCellFDD"));
			return false;
		}

		cd.src.freq = 
				EUtranFrequency.getMatching("enbId = " + cd.src.enb.eNBId + " AND arfcnValueEUtranDl = " + cd.src.freqRel.arfcnValueEUtranDl).get(0);
		cd.targ.freq = 
				EUtranFrequency.getMatching("enbId = " + cd.targ.enb.eNBId + " AND arfcnValueEUtranDl = " + cd.targ.freqRel.arfcnValueEUtranDl).get(0);
				
		
		List<ExternalENodeBFunction> srcEENBList = 
				ExternalENodeBFunction.getMatching("ownerId = " + cd.src.enb.eNBId + " AND targetId = " + cd.targ.enb.eNBId);
		if ( srcEENBList.size() == 1 ) {
			cd.src.eenb = srcEENBList.get(0); 
		}
		List<ExternalENodeBFunction> targEENBList = 
				ExternalENodeBFunction.getMatching("ownerId = " + cd.targ.enb.eNBId + " AND targetId = " + cd.src.enb.eNBId);
		if ( targEENBList.size() == 1) {
			cd.targ.eenb = targEENBList.get(0);			
		}
		logger.finer("srcEENBList.size=" + srcEENBList.size() + " targEENBList.size=" + targEENBList.size());
		
		List<TermPointToENB> srcTPTENBList = 
				TermPointToENB.getMatching("ownerId = " + cd.src.enb.eNBId + " AND targetId = " + cd.targ.enb.eNBId);
		List<TermPointToENB> targTPTENBList = 
				TermPointToENB.getMatching("ownerId = " + cd.targ.enb.eNBId + " AND targetId = " + cd.src.enb.eNBId);
		logger.finer("srcTPTENBList.size=" + srcTPTENBList.size() + " targTPTENBList.size=" + targTPTENBList.size());

		// We'll only allow this to proceed if the nodes are completely un-related
		// or fully related. If the TermPointToENB is
		// present on one side and not present on the order then we return false
		if ( (srcTPTENBList.size() != 1 && targTPTENBList.size() == 1) ||
		     (srcTPTENBList.size() == 1 && targTPTENBList.size() != 1) ) {
			logger.fine(cd.src.enb.fdn + " has " + srcEENBList.size() + " matching ExternalENodeBFunction and " +
					srcTPTENBList.size() + " matching TermPointToENB and " +
                    cd.targ.enb.fdn + " has " + targEENBList.size() + " and " + targTPTENBList.size());
			return false;
			
		}
		
		if ( srcTPTENBList.size() == 0 && targTPTENBList.size() == 0 ) {
			cd.x2SetupRequired = true;
		} else {			
			cd.x2SetupRequired = false;
		}
					
		return true;
	}

	@Override
	public Set<CellIdentity> getRelatedCells(CellIdentity ci) throws Exception {
		logger.finer("getRelatedCells");
		
		Set<CellIdentity> results = new HashSet<CellIdentity>();
		
		List<EUtranCellRelation> existingRels = 
				EUtranCellRelation.getMatching("enbIdA = " + ci.enbId + " AND " + " cellIdA = " + ci.cellId);
		for ( EUtranCellRelation rel : existingRels ) {
			results.add(new CellIdentity(rel.enbIdB,rel.cellIdB));
		}

		return results;
	}

	@Override
	public boolean isRelated(CellIdentity cia, CellIdentity cib, CreateData cd)
			throws Exception {
		logger.finer("isRelated");

                return isRelated(cia,cib) || isRelated(cib,cia);
	}

        private boolean isRelated(CellIdentity cia, CellIdentity cib) throws Exception {
           String filterFmt = "(enbIdA = %d AND cellIdA = %d AND enbIdB = %d AND cellIdB = %d)";
           String filter = String.format(filterFmt, cia.enbId, cia.cellId, cib.enbId, cib.cellId);
           List<EUtranCellRelation> existingRels = EUtranCellRelation.getMatching(filter);
           if ( existingRels.size() > 0 ) {
                        return true;
           } else {
                        return false;
           }
        }
	
}
