package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;
import com.ericsson.oss.anrx2.simulator.engine.ThreadManager;

public class SetParam implements ICommandHandler {

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) throws Exception {
		String name = parameters.get("name");
		String value = parameters.get("value");
		if ( name == null || value == null ) { 
			throw new Exception("Missing arguement: Usage setParam name <paramName> value <paramValue");			
		}
		
		Config.getInstance().getProps().setProperty(name,value);
	    AtomicBoolean exitFlag = ThreadManager.getInstance().getExitFlag();
	    synchronized (exitFlag) {
	    	exitFlag.notifyAll();
	    }
		return null;
	}

}
