package com.ericsson.oss.anrx2.simulator.engine.celllocation.cart;

import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.IDistanceCalculator;

public class DistanceCalculator implements IDistanceCalculator {
	public int distance(CellLocation from, CellLocation to) {
		// longitude(-180 -> 180) * 1000000		
		// latitude (-90 -> 90)   * 1000000 
		float fromX = (from.longitude / 1000000f) + 180f;
		float fromY = (from.latitude / 1000000f) + 90f;
		float toX = (to.longitude / 1000000f) + 180f;
		float toY = (to.latitude / 1000000f) + 90f;
		
		float x = Math.abs(fromX-toX);
		float y = Math.abs(fromY-toY);
		// ~ 13546 distance between 2 adjacent cells
		int distance = (int) (Math.sqrt((x*x) + (y*y)) * 1000000);
		return distance;						
	}

}
