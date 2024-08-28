package com.ericsson.oss.anrx2.simulator.engine.create.random.pairsims;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;

public class AllPairs implements IRandomRelationCreator {
	private final List<PairedSims> pairedSims;
	Iterator<PairedSims> itr;
	
	public AllPairs(List<String[]> pairSimNames) throws Exception {
		pairedSims = new LinkedList<PairedSims>();
		for ( String[] pairSimName : pairSimNames ) {
			pairedSims.add(new PairedSims(pairSimName[0],pairSimName[1]));
		}
		itr = pairedSims.iterator();
	}
	
	public void createRelation() throws Exception {
		if ( ! itr.hasNext() ) {
			itr = pairedSims.iterator();
		}
		PairedSims ps = itr.next();
		ps.createRandomRelation();
	}
}
