package com.ericsson.oss.anrx2.simulator.engine.celllocation;

public class CellLocation {
	public final int latitude;
	public final int longitude;
	public final int altitude;
	
	public CellLocation( int latitude, int longitude, int altitude ) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.altitude = altitude;
	}
	
	public String toString() {
		return String.valueOf(latitude) + "," + String.valueOf(longitude);
	}
}
