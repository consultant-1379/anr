package com.ericsson.oss.anrx2.simulator.engine.celllocation.xy;

import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.IDistanceCalculator;

public class DistanceCalculator implements IDistanceCalculator {
	public int distance(CellLocation from, CellLocation to) {
		int x = Math.abs(from.longitude - to.longitude);		
		int y = Math.abs(from.latitude - to.latitude);
		int distance = (int) Math.sqrt( (double)((x*x) + (y*y)) );
		return distance;				
	}
}
