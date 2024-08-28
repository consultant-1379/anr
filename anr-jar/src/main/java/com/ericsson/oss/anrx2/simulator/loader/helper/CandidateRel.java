package com.ericsson.oss.anrx2.simulator.loader.helper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ericsson.oss.anrx2.simulator.db.CandNeighborRel;
import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.IStorable;
import com.ericsson.oss.anrx2.simulator.loader.ILoaderHelper;
import com.ericsson.oss.anrx2.simulator.loader.Loader;

public class CandidateRel implements ILoaderHelper {
	@Override
	public void processData( Object data, Db db ) throws Exception {
		@SuppressWarnings("unchecked")
		List<int[]> candRelData = (List<int[]>)data;
		Date now = new Date();
		List<IStorable> candRels = new ArrayList<IStorable>(candRelData.size());
		for ( int[] crel : candRelData ) {			
			candRels.add(new CandNeighborRel(crel[0],crel[1],crel[2],crel[3],crel[4],0,now));
		}
		Loader.log("Store Candidate Relations: " + candRels.size());
		db.loadStorable(candRels);		
	}

}
