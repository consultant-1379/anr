package com.ericsson.oss.anrx2.simulator.netsim;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Config;

public class SimConnectionFactory {
	private final static Logger logger = Logger.getLogger(SimConnectionFactory.class.getName()); 
	
	private static SimConnectionFactory inst;
	private Map<String,String> simHosts = new HashMap<String,String>();
	private Map<String,LinkedList<SimConnection>> scPool = new HashMap<String,LinkedList<SimConnection>>();
	
	private AtomicInteger numCreated = new AtomicInteger();
	private AtomicInteger numPooled = new AtomicInteger();
	private AtomicInteger numClosed = new AtomicInteger();
	
	public static synchronized SimConnectionFactory getInstance() {
		if ( inst == null ) {
			inst = new SimConnectionFactory();
		}
		return inst;
	}
	
	public SimConnection getConnection(String simName) throws Exception {
		String simHost = simHosts.get(simName);
		if ( simHost == null ) {
			simHost = Config.getInstance().getProps().getProperty("simhost." + simName);
			if ( simHost == null ) {
				throw new IllegalStateException("No defined simhost for " + simName);
			} else {
				simHosts.put(simName,simHost);
			}
		}
		
		SimConnection sc;
		synchronized (scPool) {
			LinkedList<SimConnection> simHostConnections = scPool.get(simName);
			if ( simHostConnections == null ) {
				simHostConnections = new LinkedList<SimConnection>();
				scPool.put(simName,simHostConnections);
			}
			sc = simHostConnections.pollFirst();
		}
		
		if ( sc == null ) { // No pooled connection available
			sc = new SimConnection(simHost,simName);
			numCreated.incrementAndGet();
		} else { 
			numPooled.decrementAndGet();
		}
		logger.fine("getConnection numCreated " + numCreated + " numPooled " + numPooled + " numClosed " + numClosed);
		return sc;
	}
	
	public void returnConnection(SimConnection sc, boolean reuse) {
		if ( reuse ) {
			synchronized (scPool) {
				LinkedList<SimConnection> simHostConnections = scPool.get(sc.getSimName());
				simHostConnections.add(sc);
				numPooled.incrementAndGet();
			}
		} else {
			sc.close();
			numClosed.incrementAndGet();
		}
		logger.fine("returnConnection numCreated " + numCreated + " numPooled " + numPooled + " numClosed " + numClosed);		
	}
	
	public void shutdown() throws Exception {
		synchronized (scPool) {
			for ( Map.Entry<String, LinkedList<SimConnection>> entry : scPool.entrySet() ) {
				for ( SimConnection sc : entry.getValue() ) {
					sc.close();
				}
			}
			scPool.clear();
		}
	}
}
