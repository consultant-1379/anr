package com.ericsson.oss.anrx2.simulator.engine.create.reltype;

import java.util.Set;

import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;
import com.ericsson.oss.anrx2.simulator.engine.create.CreateData;

public interface IRelationCreator {
	public void createRelation(CreateData cd) throws Exception;
	public boolean setup(CreateData cd) throws Exception;	
	public Set<CellIdentity> getRelatedCells(CellIdentity ci) throws Exception;
	public boolean isRelated(CellIdentity cia, CellIdentity cib, CreateData cd) throws Exception;
}
