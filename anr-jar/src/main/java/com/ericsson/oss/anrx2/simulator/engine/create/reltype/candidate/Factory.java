package com.ericsson.oss.anrx2.simulator.engine.create.reltype.candidate;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ericsson.oss.anrx2.simulator.db.CandNeighborRel;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.ThreadManager;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreatorFactory;

public class Factory implements IRelationCreatorFactory {
	public Factory() throws Exception {
		AtomicBoolean exitFlag = ThreadManager.getInstance().getExitFlag();
		@SuppressWarnings("unchecked")
		List<String> simList = (List<String>)(Config.getInstance().getProps().get("sims"));
        RelationPersistanceManager relPersistMgr = new RelationPersistanceManager(exitFlag, simList);
        Thread perisitanceThread = new Thread(relPersistMgr);
        perisitanceThread.start();
        ThreadManager.getInstance().registerThread(perisitanceThread);		
	}
	@Override
	public IRelationCreator makeRelationCreator() throws Exception {
		// TODO Auto-generated method stub
		return new CandidateRelationCreator();
	}

	@Override
	public List<ICellRelation> getRelations(String filter) throws Exception {
		List<ICellRelation> results = new LinkedList<ICellRelation>();
		results.addAll(EUtranCellRelation.getMatching(filter));
		results.addAll(CandNeighborRel.getMatching(filter));		
		return results;
	}
	
}
