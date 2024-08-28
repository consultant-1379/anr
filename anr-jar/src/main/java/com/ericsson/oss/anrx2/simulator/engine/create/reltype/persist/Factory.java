package com.ericsson.oss.anrx2.simulator.engine.create.reltype.persist;

import java.util.LinkedList;
import java.util.List;

import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.IRelationCreatorFactory;

public class Factory implements IRelationCreatorFactory {

	@Override
	public IRelationCreator makeRelationCreator() throws Exception {
		// TODO Auto-generated method stub
		return new PersistentRelationCreator();
	}

	@Override
	public List<ICellRelation> getRelations(String filter) throws Exception {
		List<ICellRelation> results = new LinkedList<ICellRelation>();
		results.addAll(EUtranCellRelation.getMatching(filter));
		return results;
	}	

}
