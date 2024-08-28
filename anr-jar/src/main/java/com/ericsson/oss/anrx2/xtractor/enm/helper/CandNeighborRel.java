package com.ericsson.oss.anrx2.xtractor.enm.helper;

import java.util.List;

import javax.persistence.EntityManager;

import com.ericsson.oss.anrx2.xtractor.enm.AttributeHelper;
import com.versant.jpa.generic.DatabaseClass;
import com.versant.jpa.generic.DatabaseField;
import com.versant.jpa.generic.DatabaseObject;

public class CandNeighborRel implements AttributeHelper {
	DatabaseField[] fields = null;

	@Override
	public String processAttribute(Object fieldValue, EntityManager em) throws Exception {
		final DatabaseClass clazz = DatabaseClass.forName("ns_ERBS_NODE_MODEL.Et_CandNeighborRelInfo", em.getEntityManagerFactory());
		fields = new DatabaseField[] {			
				clazz.getDeclaredField("at_physicalLayerSubCellId"),
				clazz.getDeclaredField("at_physicalLayerCellIdGroup"),
				clazz.getDeclaredField("at_cellId"),
				clazz.getDeclaredField("at_enbId"),
				clazz.getDeclaredField("at_mcc"),
				clazz.getDeclaredField("at_mnc"),
				clazz.getDeclaredField("at_mncLength"),
				clazz.getDeclaredField("at_tac")
		};
		
		StringBuffer result = new StringBuffer();
		@SuppressWarnings("unchecked")
		List<DatabaseObject> candNeighborRel = (List<DatabaseObject>)fieldValue;
		for ( DatabaseObject oneCRel : candNeighborRel ) {
			if ( result.length() > 0 ) {
				result.append(":");
			}
			for ( int attrIndex = 0; attrIndex < fields.length; attrIndex++ ) {
				if ( attrIndex > 0 ) {
					result.append(";");
				}
				result.append(fields[attrIndex].get(oneCRel));
			}
		}
		
		return result.toString();
	}
}
