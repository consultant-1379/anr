package com.ericsson.oss.anrx2.simulator.engine;

import java.util.Map;

public interface ICommandHandler {
	public String[] execute(Map<String,String> parameters, Engine engine) throws Exception;
}
