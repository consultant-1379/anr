package com.ericsson.oss.anrx2.simulator.engine.command;

import java.util.Map;
import java.util.Random;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;
import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteNode;
import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteParams;

public class NodeDelete implements ICommandHandler {
	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) throws Exception {
		int enbId = Integer.parseInt(parameters.get("enbId"));
		DeleteNode dn = new DeleteNode(enbId,new DeleteParams(),
									   Config.getInstance().getPlmnId(),new Random());
	    dn.processNode();
		return null;
	}
}
