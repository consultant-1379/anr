package com.ericsson.oss.anrx2.xtractor.ossrc;

import com.versant.fund.FundSession;
import com.versant.fund.Handle;

public interface HandleHelper {
	public String processHandle( Handle handle, FundSession session ) throws Exception;
}
