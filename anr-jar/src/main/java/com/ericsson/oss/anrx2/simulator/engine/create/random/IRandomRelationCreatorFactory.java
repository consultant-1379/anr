package com.ericsson.oss.anrx2.simulator.engine.create.random;

/**
 * 
 * Interface to construct the object that will be used to create new relations. The IRandomRelationCreator is
 * responsible for selecting the pair of cells that will be connected by the new relation
 *
 */
public interface IRandomRelationCreatorFactory {
	public IRandomRelationCreator create() throws Exception;
}
