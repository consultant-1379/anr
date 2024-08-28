package com.ericsson.oss.anrx2.simulator.engine.delete;

import com.ericsson.oss.anrx2.simulator.engine.Config;

public class DeleteParams {
	public final long cellKeepTime;
	public final long relKeepTime;
	public final long funcKeepTime;
	
	public final int nearDist;
	public final int farDist;
		
	public final int relThreshold;

	public DeleteParams( long cellKeepTime, long relKeepTime, long funcKeepTime,
			int nearDist, int farDist,
			int relThreshold ) {
		this.cellKeepTime = cellKeepTime;
		this.relKeepTime = relKeepTime;
		this.funcKeepTime = funcKeepTime;
		this.nearDist = nearDist;
		this.farDist = farDist;
		this.relThreshold = relThreshold;
	}
	
	public DeleteParams() throws Exception {
		cellKeepTime = Long.parseLong(Config.getInstance().getManditoryParam("ExternalEUtranCellFDD.keepTime")) * 60000L;
		relKeepTime = Long.parseLong(Config.getInstance().getManditoryParam("EUtranCellRelation.keepTime")) * 60000L;
		funcKeepTime = Long.parseLong(Config.getInstance().getManditoryParam("ExternalENodeBFunction.keepTime")) * 60000L;
		
		nearDist = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.nearDist"));
		farDist = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.farDist"));
		relThreshold = Integer.parseInt(Config.getInstance().getManditoryParam("EUtranCellRelation.deleteThreshold"));				
	}
}
