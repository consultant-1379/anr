package com.ericsson.oss.anrx2.simulator.engine.delete.reltype;

import java.sql.Timestamp;
import java.util.Random;

import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteParams;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnection;

public interface IRelationDeleter {
	public int delete(SimConnection simConn, Timestamp lastUpdated, 
			int enbId, String meId, 
			String[] plmnId, Random rnd, 
			boolean cleanNode, DeleteParams delParams) throws Exception;
}
