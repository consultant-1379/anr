package com.ericsson.oss.anrx2.simulator.loader;

import com.ericsson.oss.anrx2.simulator.db.Db;

public interface ILoaderHelper {
	public void processData( Object data, Db db ) throws Exception;
}
