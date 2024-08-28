package com.ericsson.oss.anrx2.simulator.engine.create.random.pairsims;

import java.util.LinkedList;
import java.util.List;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.DistanceCalculator;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreatorFactory;

public class Factory implements IRandomRelationCreatorFactory {

	@Override
	public IRandomRelationCreator create() throws Exception {
		
    	DistanceCalculator.impl = new com.ericsson.oss.anrx2.simulator.engine.celllocation.xy.DistanceCalculator();
		List<String[]> pairSimNames = Config.getInstance().getPairs();
		List<String> sims = new LinkedList<String>();
		for ( String[] pairSim : pairSimNames) {			
			sims.add(pairSim[0]);
			sims.add(pairSim[1]);    			
		}
        Config.getInstance().getProps().put("sims", sims);
		
        return new AllPairs(pairSimNames);
	}

}
