package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;

import com.ericsson.oss.anrx2.simulator.engine.BlackList;
import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;

public class DumpBlackList implements ICommandHandler {

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) {
		BlackList.getInstance().logList();	
		return null;
	}

}
