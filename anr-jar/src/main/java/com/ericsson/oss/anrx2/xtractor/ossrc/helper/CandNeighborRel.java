package com.ericsson.oss.anrx2.xtractor.ossrc.helper;


import com.ericsson.oss.anrx2.xtractor.ossrc.HandleHelper;
import com.versant.fund.Attr;
import com.versant.fund.AttrHandleArray;
import com.versant.fund.FundSession;
import com.versant.fund.Handle;

public class CandNeighborRel implements HandleHelper {
	AttrHandleArray elementsAttr = null;
	Attr[] attrs = null;
	@Override
	public String processHandle(Handle handle, FundSession session) throws Exception {
		if ( elementsAttr == null ) {
			elementsAttr = session.newAttrHandleArray("elements");
			attrs = new Attr[] { 
					session.newAttrInt("physicalLayerSubCellId"),
					session.newAttrInt("physicalLayerCellIdGroup"),
					session.newAttrInt("cellId"),
					session.newAttrInt("enbId"),
					session.newAttrInt("mcc"),
					session.newAttrInt("mnc"),
					session.newAttrInt("mncLength"),
					session.newAttrInt("tac")
			};
		}
		
		Handle[] elements = handle.get(elementsAttr);
		StringBuffer result = new StringBuffer();
		
		for ( int elementIndex = 0; elementIndex < elements.length; elementIndex++ ) {
			if ( elementIndex > 0 ) {
				result.append(":");
			}
			for ( int attrIndex = 0; attrIndex < attrs.length; attrIndex++ ) {
				if ( attrIndex > 0 ) {
					result.append(";");
				}
				result.append(elements[elementIndex].getAscii(attrs[attrIndex]));
			}
		}
		
		return result.toString();
	}
}
