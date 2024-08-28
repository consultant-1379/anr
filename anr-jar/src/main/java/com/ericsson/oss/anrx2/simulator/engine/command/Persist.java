package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;
import java.util.Random;

import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;
import com.ericsson.oss.anrx2.simulator.engine.create.reltype.candidate.CandidatePersistenceRunner;


public class Persist implements ICommandHandler {

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) {
		try {
			Integer enbId = Integer.parseInt(parameters.get("enbId"));
			CandidatePersistenceRunner cpr = new CandidatePersistenceRunner(enbId, new Random());
			cpr.run();
			return null;
		} catch ( Exception e ) {
			return new String[] { e.toString() };
		}
	}
}
