package com.ericsson.oss.anrx2.simulator.engine.celllocation;

public interface IDistanceCalculator {
	public int distance( CellLocation from, CellLocation to );
}
