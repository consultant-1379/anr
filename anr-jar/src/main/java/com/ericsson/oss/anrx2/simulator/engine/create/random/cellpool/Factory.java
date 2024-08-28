package com.ericsson.oss.anrx2.simulator.engine.create.random.cellpool;

import java.util.Arrays;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.DistanceCalculator;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreatorFactory;

public class Factory implements IRandomRelationCreatorFactory {

	@Override
	public IRandomRelationCreator create() throws Exception {
		DistanceCalculator.impl = new com.ericsson.oss.anrx2.simulator.engine.celllocation.cart.DistanceCalculator();
		
		String simListStr = Config.getInstance().getManditoryParam("poolSims");
		String simList[] = simListStr.split(","); 
        Config.getInstance().getProps().put("sims", Arrays.asList(simList));

        return new CellPool(simList);        
	}

}
