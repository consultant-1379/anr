package com.ericsson.oss.anrx2.simulator.engine.create;

import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.NodeLockFactory;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.RelationCreatorFactory;

public class OneShotCreator {
	private final static Logger logger = Logger.getLogger(OneShotCreator.class.getName());
	
	public void createThisRelation( int enbIdA, int cellIdA, int enbIdB, int cellIdB ) throws Exception {
		logger.info("createThisRelation");
		CellIdentity cia = new CellIdentity(enbIdA,cellIdA);
		CellIdentity cib = new CellIdentity(enbIdB,cellIdB);
		CreateData cd = new CreateData();
		
		Lock lockA = NodeLockFactory.getInstance().getLock(cia.enbId);
		lockA.lock();
		cd.lockNodeA = lockA;
		
		Lock lockB = NodeLockFactory.getInstance().getLock(cib.enbId);
		lockB.lock();
		cd.lockNodeB = lockB;
		
		cd.src.cellIdent = cia;
		cd.targ.cellIdent = cib;
		
		try {
			IRelationCreator rc = RelationCreatorFactory.getInstance().makeRelationCreator();
			if ( rc.setup(cd) ) {
				rc.createRelation(cd);
			} 
		} finally {
			lockA.unlock();
			lockB.unlock();
		}
	}
}
