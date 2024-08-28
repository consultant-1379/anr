package com.ericsson.oss.anrx2.simulator.db;

public class DbChange {
	protected ChangeType type;
	protected IStorable data;
	
	public DbChange( ChangeType type, IStorable data ) {
		this.type = type;
		this.data = data;
	}
}
