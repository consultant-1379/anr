package com.ericsson.oss.anrx2.simulator.engine.create.reltype;

import java.util.List;

import com.ericsson.oss.anrx2.simulator.db.ICellRelation;

/**
 * 
 * Interface to construct the object that will be used to create the relation between the specified cells
 */
public interface IRelationCreatorFactory {
	IRelationCreator makeRelationCreator() throws Exception;
	List<ICellRelation> getRelations(String filter) throws Exception;
}
