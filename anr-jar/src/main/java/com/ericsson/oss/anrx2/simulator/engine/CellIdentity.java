package com.ericsson.oss.anrx2.simulator.engine;

public class CellIdentity {
	public final int enbId;
	public final int cellId;
	public CellIdentity(int enbId, int cellId) {
		this.enbId = enbId;
		this.cellId = cellId;
	}
		
	public String toString() {
		return enbId + "-" + cellId;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cellId;
		result = prime * result + enbId;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CellIdentity other = (CellIdentity) obj;
		if (cellId != other.cellId)
			return false;
		if (enbId != other.enbId)
			return false;
		return true;
	}
	
	
}
