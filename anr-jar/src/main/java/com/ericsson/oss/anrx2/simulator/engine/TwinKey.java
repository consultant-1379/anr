package com.ericsson.oss.anrx2.simulator.engine;

public class  TwinKey {
	public final int a;
	public final int b;
	
	public TwinKey(int a, int b) {
		this.a = a;
		this.b = b;		
	}
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwinKey other = (TwinKey) obj;
		if (a != other.a)
			return false;
		if (b != other.b)
			return false;
		return true;
	}
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + a;
		result = prime * result + b;
		return result;
	}
	
	public String toString() {
		return "[a=" + a + ", b=" + b + "]";
	}	
}