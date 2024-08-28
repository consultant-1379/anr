package com.ericsson.oss.anrx2.simulator.engine;

public class FdnHelper {

	public static String getMoId(String fdn,String moType) {
		int startId = fdn.indexOf("," + moType + "=") +1;
		int endId = fdn.indexOf(",",startId);
		String rdn;
		if ( endId == -1 ) {
			rdn = fdn.substring(startId);
		} else {
			rdn = fdn.substring(startId,endId);
		}
		return rdn.split("=")[1];
	}

}
