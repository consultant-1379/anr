package com.ericsson.oss.anrx2.simulator.db;

import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;

public interface ICellRelation extends IStorable {
	public CellIdentity getCellA();
	public CellIdentity getCellB();
	public int getDistance();
	public void setDistance(int dist);
}
