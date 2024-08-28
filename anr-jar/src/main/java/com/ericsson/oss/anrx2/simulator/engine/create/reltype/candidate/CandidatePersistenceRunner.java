package com.ericsson.oss.anrx2.simulator.engine.create.reltype.candidate;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
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
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.TermPointToENB;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.CreatedByEutran;
import com.ericsson.oss.anrx2.simulator.engine.FdnHelper;
import com.ericsson.oss.anrx2.simulator.engine.IdFactory;
import com.ericsson.oss.anrx2.simulator.engine.NodeLockFactory;
import com.ericsson.oss.anrx2.simulator.engine.TwinKey;
import com.ericsson.oss.anrx2.simulator.engine.create.CreateData;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnectionFactory;
import com.ericsson.oss.anrx2.simulator.netsim.candidate.CandRelHelper;

public class CandidatePersistenceRunner implements Runnable {
	private final static Logger logger = Logger.getLogger(CandidatePersistenceRunner.class.getName());
	private final long relKeepTime;
	private final int nearDist;
	private final int farDist;
	private final int delRelThreshold;
	private final int persistRelThreshold;
	private final String[] plmnId;
	
	private LinkedList<DbChange> dbChanges;
	private KertayleSession ks; 

	public CandidatePersistenceRunner(int enbId, Random rnd) throws Exception {
		this.enbId = enbId;
		this.rnd = rnd;

		relKeepTime = Long.parseLong(Config.getInstance().getManditoryParam("EUtranCellRelation.keepTime")) * 60000L;

		nearDist = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.nearDist"));
		farDist = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.farDist"));
		delRelThreshold = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.deleteThreshold"));
		persistRelThreshold = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.presistCandidateThreshold"));

		plmnId = Config.getInstance().getPlmnId();		
	}

	private final int enbId;
	private final Random rnd;
	private ENodeBFunction enb;
	private HashMap<TwinKey, EUtranFreqRelation> allFreqRel;
	private HashMap<Integer, EUtranCellFDD> allCells;
	private String meId;
	private SimConnection simConn;
	
	public void run() {
		logger.fine("run: enbId=" + enbId);
		try {
			enb = ENodeBFunction.getMatching("eNBId = " + enbId ).get(0);
			String meConFdn = enb.fdn.substring(0,enb.fdn.indexOf(",ManagedElement"));
			meId = meConFdn.substring(meConFdn.lastIndexOf("=")+1);
			logger.info(meId + ": Started processing");
				
			Lock nodeLock = NodeLockFactory.getInstance().getLock(enbId);
			nodeLock.lock();

			try {
				String simlation = enb.sim;
				simConn = SimConnectionFactory.getInstance().getConnection(simlation);

				runAlgorithm(simConn);
				
			    SimConnectionFactory.getInstance().returnConnection(simConn,true);
			    simConn = null;
			} finally {
				nodeLock.unlock();

				if ( simConn != null ) {
					SimConnectionFactory.getInstance().returnConnection(simConn,false);
				}
			}			
			logger.info(meId + ": Completed processing");
		} catch ( Throwable t ) {
			logger.log(Level.SEVERE, "Failed for " + enbId, t);
			BlackList.getInstance().add(enbId);
		}			
	}
	
	private void runAlgorithm(SimConnection simConn) throws Exception {
		logger.fine("runAlgorithm: enbId=" + enbId);

		dbChanges = new LinkedList<DbChange>();
		ks = new KertayleSession();		


		allFreqRel = new HashMap<TwinKey,EUtranFreqRelation>();
		for ( EUtranFreqRelation freqRel : EUtranFreqRelation.getMatching("enbId = " + enbId) ) {
			allFreqRel.put(new TwinKey(freqRel.cellId,freqRel.arfcnValueEUtranDl), freqRel);
		}

		allCells = new HashMap<Integer,EUtranCellFDD>();
		for ( EUtranCellFDD cell : EUtranCellFDD.getMatching("enbId = " + enbId) ) {
			allCells.put(cell.cellId, cell);
		}

		// Get all candidate relations and then group then by containing EUtranFreqRelation
		List<CandNeighborRel> cRels = CandNeighborRel.getMatching("enbIdA = " + enbId); 
		// CandNeighborRel grouped by containing EUtranFreqRelation		
		Map<TwinKey,List<CandNeighborRel>> candPerFreqRel = new HashMap<TwinKey,List<CandNeighborRel>>();
		for ( CandNeighborRel cRel : cRels ) {
			TwinKey freqRel = new TwinKey(cRel.cellIdA,cRel.arfcnValueEUtranDl);
			List<CandNeighborRel> cRelsInFreqRel = candPerFreqRel.get(freqRel);
			if ( cRelsInFreqRel == null ) {
				cRelsInFreqRel = new LinkedList<CandNeighborRel>();
				candPerFreqRel.put(freqRel,cRelsInFreqRel);
			}	
			cRelsInFreqRel.add(cRel);
		}
		
				
		Date now = new Date();
		Date deleteTime = new Date(System.currentTimeMillis()-relKeepTime);
		
		candidateToPersist(candPerFreqRel,deleteTime,now);
		
		persistToCandidate(candPerFreqRel,deleteTime,now);
		
		
	}

	private void candidateToPersist(
			Map<TwinKey, List<CandNeighborRel>> candPerFreqRel, Date deleteTime, Date now) throws Exception {		
		logger.fine("candidateToPersist");
		
		// Candidate Relations that need to be converted to EUtranFreqRelation
		List<CandNeighborRel> cRelsToPersist = new LinkedList<CandNeighborRel>();
		// Tracks which EUtranFreqRelation.candidateRels have been modified
		Set<TwinKey> modifiedCandRel = new HashSet<TwinKey>(); 
		
		Map<Integer,Lock> nodeLocks = new HashMap<Integer,Lock>();
		int countDeletedOld = 0;
		for ( Map.Entry<TwinKey, List<CandNeighborRel>> entry : candPerFreqRel.entrySet() ) {
			TwinKey freqRel = entry.getKey();
			List<CandNeighborRel> cRelsInFreqRel = entry.getValue();

			for ( Iterator<CandNeighborRel> cRelItr = cRelsInFreqRel.iterator(); cRelItr.hasNext();  ) {
				CandNeighborRel cRel = cRelItr.next();
				if  ( cRel.dist < nearDist ) {
					logger.fine("candidateToPersist: persisting near " + cRel); 
					// Near relations are always persisted

					// We need the lock for the other node so get it now to be sure that
					// we can persist the relation
					boolean gotLock = true;
					if ( ! nodeLocks.containsKey(cRel.enbIdB) ) {
						Lock nodeLock = NodeLockFactory.getInstance().getLock(cRel.enbIdB);
						if ( nodeLock.tryLock() ) {
							logger.fine("candidateToPersist: Near Locked " + cRel.enbIdB);
							nodeLocks.put(cRel.enbIdB,nodeLock);
						} else {
							gotLock = false;
						}
					}
					if ( ! gotLock ) {
						logger.warning("candidateToPersist: Failed to get lock for enbId " + cRel.enbIdB);
						continue;
					}

					// removing it from the candNeighborRel attribute
					cRelItr.remove();

					// Remove from candNeighborRel table
					dbChanges.add(new DbChange(ChangeType.DELETE,cRel));

					// Indicate that the EUtranFreqRelation has been updated (a CandNeighborRel is being removed as 
					// 	being converted to a EUtranFreqRelation)
					modifiedCandRel.add(freqRel);

					// Add this CandNeighborRel to the list to be converted to a EUtranFreqRelation
					cRelsToPersist.add(cRel);
				} else if ( cRel.timeOfCreation.before(deleteTime) ) {
					logger.fine("candidateToPersist: deleting old " + cRel);
					countDeletedOld++;
					// Candidates older then the deleteTime are removed

					// removing it from the candNeighborRel attribute
					cRelItr.remove();
					
					// Remove from candNeighborRel table
					dbChanges.add(new DbChange(ChangeType.DELETE,cRel));
					
					
					// Indicate that the EUtranFreqRelation has been updated (a CandNeighborRel is being removed as 
					// it is being deleted(				
					modifiedCandRel.add(freqRel);					
				} else if ( cRel.dist < farDist && persistRelThreshold > rnd.nextInt(1000) ) {
					logger.fine("candidateToPersist: presisting middle " + cRel);								
					// Middle distances relations might be promoted based on persistRelThreshold

					boolean gotLock = true;
					if ( ! nodeLocks.containsKey(cRel.enbIdB) ) {
						Lock nodeLock = NodeLockFactory.getInstance().getLock(cRel.enbIdB);
						if ( nodeLock.tryLock() ) {
							logger.fine("candidateToPersist: Middle Locked " + cRel.enbIdB);							
							nodeLocks.put(cRel.enbIdB,nodeLock);
						} else {
							gotLock = false;
						}
					}
					if ( ! gotLock ) {
						logger.warning("candidateToPersist: Failed to get lock for enbId " + cRel.enbIdB);
						continue;
					}
					
					// removing it from the candNeighborRel attibute
					cRelItr.remove();
					
					// Remove from candNeighborRel table
					dbChanges.add(new DbChange(ChangeType.DELETE,cRel));
					
					// Indicate that the EUtranFreqRelation has been updated (a CandNeighborRel is being removed as 
					// being converted to a EUtranFreqRelation)
					modifiedCandRel.add(freqRel);

					// Add this CandNeighborRel to the list to be converted to a EUtranFreqRelation
					cRelsToPersist.add(cRel);
				} else {
					// We keeping this as a candidate relation so just add it into cRelsInFreqRel
					logger.finest("candidateToPersist: keeping as candidate " + cRel + " dist " + cRel.dist);													
				}
			}
		}

		logger.fine("candidateToPersist: cRelsToPersist.size=" + cRelsToPersist.size() + ", modifiedCandRel.size=" + modifiedCandRel.size());		
		if ( cRelsToPersist.size() == 0 && modifiedCandRel.size() == 0) {
			return;
		}

		Map <Integer, TermPointToENB> existingTPTENB = new HashMap<Integer,TermPointToENB>();
		for ( TermPointToENB tptENB : TermPointToENB.getMatching("ownerId = " + enb.eNBId) ) {
			existingTPTENB.put(tptENB.targetEnbId, tptENB);
		}
		
		try {
			// Write the updated EUtranFreqRelation.candNeighborRel to DB and simulations
			// Now update the candNeighborRel in netsim
			// in order for the query to work we write the changes to the db first
			Db.getInstance().loadChanges(dbChanges); dbChanges.clear();
			for ( TwinKey freqRelId : modifiedCandRel ) {
				List<CandidateRelatedCellData> cRelsData = 
						CandidateRelatedCellData.getCandidateRelated(enbId,freqRelId.a,freqRelId.b);
				String srcFreqRelFdn = allCells.get(freqRelId.a).fdn + ",EUtranFreqRelation=" +
						allFreqRel.get(freqRelId).rdnId;
				CandRelHelper.setCandRel(srcFreqRelFdn, cRelsData, now, plmnId, ks,enb.neMIMVersion);
			
			}
		
			simConn.execKertayle(meId, ks, false);					

			// Now write the persistent relations
			int convertedCount = 0;
			Lock thisNodeLock = NodeLockFactory.getInstance().getLock(this.enbId); 
			for ( CandNeighborRel cRel : cRelsToPersist) {
				logger.info("Persisting " + cRel);
				// Create EUtranCellRelation
				String srcEENBfdn = enb.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + IdFactory.nodeId(plmnId,cRel.enbIdB);				
				String srcEEUCfdn = srcEENBfdn + ",ExternalEUtranCellFDD=" + IdFactory.cellId(plmnId,cRel.enbIdB, cRel.cellIdB);
				
				String srcFreqRelFdn = allCells.get(cRel.cellIdA).fdn + ",EUtranFreqRelation=" +
						allFreqRel.get(new TwinKey(cRel.cellIdA,cRel.arfcnValueEUtranDl)).rdnId;				
				String srcRelRdnId = IdFactory.cellId(plmnId,cRel.enbIdB, cRel.cellIdB);
				String srcRelFdn = srcFreqRelFdn + ",EUtranCellRelation=" + srcRelRdnId; 
				logger.fine("Creating " + srcRelFdn);
				EUtranCellRelation srcEUCR = new EUtranCellRelation(cRel.enbIdA, cRel.cellIdA, cRel.enbIdB, cRel.cellIdB, 
						cRel.arfcnValueEUtranDl, cRel.dist, CreatedByEutran.ANR, now);
				dbChanges.add(new DbChange(ChangeType.CREATE,srcEUCR));
				ks.createEUtranCellRelation(srcEUCR, srcRelFdn, srcEEUCfdn);

				// Create the TermPointToENB on this node
				if ( ! existingTPTENB.containsKey(cRel.enbIdB) ) {
					TermPointToENB tptENB = new TermPointToENB(cRel.enbIdA,cRel.enbIdB,CreatedByEutran.X2);
					existingTPTENB.put(cRel.enbIdB, tptENB);
					dbChanges.add(new DbChange(ChangeType.CREATE,tptENB));
					String tptENBfdn =  srcEENBfdn + ",TermPointToENB=" + IdFactory.nodeId(plmnId,cRel.enbIdB);
					logger.fine("candidateToPersist: Creating " + tptENBfdn);					
					ks.createTermPointToENB(tptENBfdn,now,"");
				}
				
				Db.getInstance().loadChanges(dbChanges); dbChanges.clear();
				simConn.execKertayle(meId, ks, false);					
				
				// Create the candidate relation back to this this cell on the other node
				boolean createdRelationOnTarget = false;
				String filter = "enbIdA = " + cRel.enbIdB + " AND cellIdA = " + cRel.cellIdB + 
						" AND enbIdB = " + cRel.enbIdA + " AND cellIdB = " + cRel.cellIdA;
				if ( EUtranCellRelation.getMatching(filter).size() == 0 &&
					CandNeighborRel.getMatching(filter).size() == 0 ) {
					CreateData cd = new CreateData();
					cd.lockNodeA = nodeLocks.get(cRel.enbIdB);				
					cd.lockNodeB = thisNodeLock;			
					cd.src.cellIdent = new CellIdentity(cRel.enbIdB, cRel.cellIdB);
					cd.targ.cellIdent = new CellIdentity(cRel.enbIdA, cRel.cellIdA);
			
					CandidateRelationCreator crc = new CandidateRelationCreator();
					if  ( crc.setup(cd) ) {
						crc.createRelation(cd);
						createdRelationOnTarget = true;
					} else {
						logger.warning("Cannot persist relation " + cRel);
					}
				}
				
				// Create the TermPointToENB on the remote node
				if ( createdRelationOnTarget && 
						TermPointToENB.getMatching("ownerId = " + cRel.enbIdB + " AND targetId = " + enb.eNBId).size() == 0 ) {
					ENodeBFunction targENB = ENodeBFunction.getMatching("eNBId = " + cRel.enbIdB).get(0);
					String targMeId = FdnHelper.getMoId(targENB.fdn, "MeContext");
					SimConnection targSimConn = SimConnectionFactory.getInstance().getConnection(targENB.sim);					
					try {
						TermPointToENB tptENB = new TermPointToENB(cRel.enbIdB,cRel.enbIdA,CreatedByEutran.X2);
						dbChanges.add(new DbChange(ChangeType.CREATE,tptENB));
						String targEENBfdn = targENB.fdn + ",EUtraNetwork=1,ExternalENodeBFunction=" + IdFactory.nodeId(plmnId,cRel.enbIdA);									
						String targtptENBfdn =  targEENBfdn + ",TermPointToENB=" + IdFactory.nodeId(plmnId,cRel.enbIdA);
						logger.fine("candidateToPersist: Creating " + targtptENBfdn);
						ks.createTermPointToENB(targtptENBfdn,now,"");
					
						Db.getInstance().loadChanges(dbChanges); dbChanges.clear();
						targSimConn.execKertayle(targMeId, ks, false);
						SimConnectionFactory.getInstance().returnConnection(targSimConn, true);	
						targSimConn = null;
					} finally {
						if ( targSimConn != null ) {
							SimConnectionFactory.getInstance().returnConnection(targSimConn, false);
						}
					}
				}
				
				convertedCount++;						
			}
			if ( convertedCount > 0 || countDeletedOld > 0 ) { 
				logger.info(meId + ": Promoted " + convertedCount + " and expired " + countDeletedOld);
			}
		} finally {
			for ( Map.Entry<Integer,Lock> nodeIdLock : nodeLocks.entrySet() ) {
				logger.fine("candidateToPersist: Unlocking " + nodeIdLock.getKey());
				nodeIdLock.getValue().unlock();
			}
		}				
	}

	private void persistToCandidate(Map<TwinKey, List<CandNeighborRel>> candPerFreqRel, Date deleteTime, Date now) throws Exception {
		logger.fine("persistToCandidate: deleteTime=" + deleteTime);
		
		// Get all the cells on the node
		List<EUtranCellFDD> cellsOnNode = EUtranCellFDD.getMatching("enbId = " + enbId);
		Map<Integer,EUtranCellFDD> cellsOnNodeById = new HashMap<Integer,EUtranCellFDD>();
		for ( EUtranCellFDD cell : cellsOnNode) {
			cellsOnNodeById.put(new Integer(cell.cellId),cell);
		}
		
		List <CandNeighborRel> presistRelsToCand = new LinkedList<CandNeighborRel>();
		
		List<EUtranCellRelation> deletableRels = 
				EUtranCellRelation.getMatching("enbIdA = " + enbId + " AND timeOfCreation <= '" + TimeOfCreation.getInstance().format(deleteTime) + "'");
		Map<String,ExternalEUtranCellFDD> updatedCells = new HashMap<String,ExternalEUtranCellFDD>();	
		int convertedCount = 0;
		for ( EUtranCellRelation rel : deletableRels ) {
			boolean doDelete = true;
			if ( rel.dist < nearDist ) {
				// Relation is to a "nearby" cell so we'll never delete
				if ( logger.isLoggable(Level.FINEST)) 
					logger.finer("persistToCandidate: Skipping near " + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB + " dist=" + rel.dist);
				doDelete = false;
			} else if ( rel.dist < farDist ) {
				// Relation is to a "middle" cell so we delete it according to the relThreshold
				if ( rnd.nextInt(1000) > delRelThreshold) {
					logger.fine("persistToCandidate: Skipping mid " + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB + " dist=" + rel.dist);
					doDelete = false;
				}
			} else {
				// Relation is to a far away cell so we always delete it
				logger.fine("persistToCandidate: Deleting far" + rel.cellIdA + "->" + rel.enbIdB + "-" + rel.cellIdB + " dist=" + rel.dist);				
			}			
			if ( ! doDelete ) {
				continue;
			}
			
			// Convert the persist rel to a candidate rel
			dbChanges.add(new DbChange(ChangeType.DELETE,rel));	
			EUtranCellFDD containingCell = cellsOnNodeById.get(rel.cellIdA);
			EUtranFreqRelation containingFreqRel = EUtranFreqRelation.getMatching("enbId = " + enbId +" AND cellId = " + rel.cellIdA + " AND arfcnValueEUtranDl = " + rel.arfcnValueEUtranDl).get(0);
			String relFdn = containingCell.fdn + ",EUtranFreqRelation=" + containingFreqRel.rdnId + ",EUtranCellRelation=" + IdFactory.cellId(plmnId,rel.enbIdB,rel.cellIdB); 
			ks.deleteMO(relFdn);

			// Now get the corresponding ExternalEUtranCellFDD and update the lastUpdated
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
				relCell.lastUpdated = new Timestamp(now.getTime());
			}
			
			// Finished removing the persist rel, now add it as a candidate rel
			CandNeighborRel cRel = new CandNeighborRel(rel.enbIdA,rel.cellIdA,rel.enbIdB,rel.cellIdB,rel.arfcnValueEUtranDl,rel.dist,now);
			dbChanges.add(new DbChange(ChangeType.CREATE,cRel));
			presistRelsToCand.add(cRel);
			
			convertedCount++;
		}
		// Store the modified lastUpdated times
		for ( ExternalEUtranCellFDD relCell : updatedCells.values() ) {
			dbChanges.add(new DbChange(ChangeType.UPDATE,relCell));
		}
		
		// Tracks which EUtranFreqRelation.candidateRels have been modified
		Set<TwinKey> modifiedCandRel = new HashSet<TwinKey>(); 
		
		// Add the persistent rels that have been converted back to candidates
		// to the EUtranFreqRelation
		for ( CandNeighborRel cRel : presistRelsToCand ) {
			TwinKey freqRel = new TwinKey(cRel.cellIdA,cRel.arfcnValueEUtranDl);
			List<CandNeighborRel> cRelsInFreqRel = candPerFreqRel.get(freqRel);
			if ( cRelsInFreqRel == null ) {
				cRelsInFreqRel = new LinkedList<CandNeighborRel>();
				candPerFreqRel.put(freqRel,cRelsInFreqRel);
			}			
			
			cRelsInFreqRel.add(cRel);			
			modifiedCandRel.add(freqRel);
		}			

		// Write Changes to DB
		Db.getInstance().loadChanges(dbChanges); dbChanges.clear();
		for ( TwinKey freqRelId : modifiedCandRel ) {
			List<CandidateRelatedCellData> cRelsData = 
					CandidateRelatedCellData.getCandidateRelated(enbId,freqRelId.a,freqRelId.b);
			String srcFreqRelFdn = allCells.get(freqRelId.a).fdn + ",EUtranFreqRelation=" +
					allFreqRel.get(freqRelId).rdnId;
			CandRelHelper.setCandRel(srcFreqRelFdn, cRelsData, now, plmnId, ks,enb.neMIMVersion);
		
		}
		// Write changes to Netsim
		simConn.execKertayle(meId, ks, false);					
		
		if ( convertedCount > 0 ) {
			logger.info(meId + ": Demoted " + convertedCount);
		}
	}
	
}
