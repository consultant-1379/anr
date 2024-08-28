package com.ericsson.oss.anrx2.xtractor.enm;

import javax.persistence.EntityManager;

public interface AttributeHelper {
	public String processAttribute( Object attribute, EntityManager em) throws Exception;	
}
