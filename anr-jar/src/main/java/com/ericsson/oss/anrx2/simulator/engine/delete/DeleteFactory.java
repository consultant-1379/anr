package com.ericsson.oss.anrx2.simulator.engine.delete;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ericsson.oss.anrx2.simulator.engine.Config;

public class DeleteFactory {
	public static Runnable getDeleter(AtomicBoolean exitFlag) throws Exception {
		String deleteType = Config.getInstance().getManditoryParam("deleteType");
		@SuppressWarnings("unchecked")
		List <String> sims = (List<String>)Config.getInstance().getProps().get("sims");
		if ( "select".equals(deleteType) ) {
			return new SelectDeleter(exitFlag,sims);
		} else if ( "sequential".equals(deleteType) ) {
			return new SequentialDeleter(exitFlag,sims);
		} else {
			throw new Exception("Unknown deleteType: " + deleteType);
		}
	}
}
