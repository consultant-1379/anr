package com.ericsson.oss.anrx2.simulator.engine.delete.reltype.candidate;

import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.IRelationDeleter;
import com.ericsson.oss.anrx2.simulator.engine.delete.reltype.IRelationDeleterFactory;

public class Factory implements IRelationDeleterFactory {

	@Override
	public IRelationDeleter makeRelationDeleter() {
		// TODO Auto-generated method stub
		return new CandidateRelationDeleter();
	}

}
