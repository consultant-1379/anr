package com.ericsson.oss.anrx2.simulator.engine.create;

import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;


public class CellDistance implements Comparable<CellDistance> {
	public final CellIdentity cid;
	public final Integer dist;
	public CellDistance( CellIdentity cid, Integer dist) {
		this.cid = cid;
		this.dist = dist;
	}
	
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CellDistance other = (CellDistance) obj;
		if (cid == null) {
			if (other.cid != null)
				return false;
		} else if (!cid.equals(other.cid))
			return false;
		if (dist == null) {
			if (other.dist != null)
				return false;
		} else if (!dist.equals(other.dist))
			return false;
		return true;
	}
	
	public int compareTo(CellDistance other) {
		return dist.compareTo(other.dist);
	}
	
}