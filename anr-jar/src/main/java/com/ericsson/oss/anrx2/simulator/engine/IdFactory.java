package com.ericsson.oss.anrx2.simulator.engine;

public class IdFactory {
	public static String nodeId( String[] plmnId, int enbId ) {
		return plmnId[0] + plmnId[1] + "-" + String.valueOf(enbId);		
	}

	public static String cellId(String[] plmnId, int enbId, int cellId) {
		return plmnId[0] + plmnId[1] + "-" + String.valueOf(enbId) + "-" + String.valueOf(cellId);
	}

}
