package com.ericsson.oss.anrx2.simulator.engine.create;

import java.util.LinkedList;
import java.util.List;

public class DistanceRanges {
	public final List<CellDistance> far  = new LinkedList<CellDistance>();
	public final List<CellDistance> mid  = new LinkedList<CellDistance>();
	public final List<CellDistance> near = new LinkedList<CellDistance>();
	public int nearUsed = 0;
}