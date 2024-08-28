package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;

public class Shutdown implements ICommandHandler {
	private final static Logger logger = Logger.getLogger(Shutdown.class.getName()); 

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) {
		logger.info("shutdown called");
		engine.shutdown();
		
		return null;
	}

}
