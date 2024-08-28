package com.ericsson.oss.anrx2.simulator.engine.celllocation;


public class DistanceCalculator {
	public static IDistanceCalculator impl;
	
	public static int distance( CellLocation from, CellLocation to ) {
		return impl.distance(from, to);
	}	
}
