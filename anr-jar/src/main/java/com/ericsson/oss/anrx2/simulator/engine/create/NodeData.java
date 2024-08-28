package com.ericsson.oss.anrx2.simulator.engine.create;

import java.util.List;

import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranFreqRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFrequency;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;

public class NodeData {
	public String sim;
	public CellIdentity cellIdent;
	public String meId;
	public ENodeBFunction enb;
	
	public List<EUtranCellFDD> cells;
	public EUtranCellFDD cell;
	
	public EUtranFreqRelation freqRel;
	public EUtranFrequency freq;		
	public ExternalENodeBFunction eenb;
	
	public ExternalEUtranCellFDD eeuc;
}