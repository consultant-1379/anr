package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;

import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;
import com.ericsson.oss.anrx2.simulator.engine.create.OneShotCreator;

public class CreateRel implements ICommandHandler {

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) throws Exception {
		OneShotCreator osc = new OneShotCreator();
        osc.createThisRelation(Integer.parseInt(parameters.get("enbIdA")), 
    				Integer.parseInt(parameters.get("cellIdA")),
    				Integer.parseInt(parameters.get("enbIdB")),
    				Integer.parseInt(parameters.get("cellIdB")));
		return null;
	}

}
