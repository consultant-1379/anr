package com.ericsson.oss.anrx2.simulator.engine.create;

import java.util.concurrent.locks.Lock;

public class CreateData {
	public Lock lockNodeA;
	public Lock lockNodeB;
	
	public CreateData() {
		src = new NodeData();
		targ = new NodeData();
	}
	
	public int dist = 0;
	public final NodeData src;
	public final NodeData targ;
	public boolean x2SetupRequired = false;
	public boolean createAllEEUC = true;
}