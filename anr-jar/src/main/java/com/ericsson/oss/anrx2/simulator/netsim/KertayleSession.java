package com.ericsson.oss.anrx2.simulator.netsim;

import java.io.PrintWriter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.CreatedByEutran;

public class KertayleSession {
	protected List<String> lines = new LinkedList<String>();
	private final static Logger logger = Logger.getLogger(KertayleSession.class.getName()); 
	
	public void deleteMO( String fdn ) {
		String ldn = stripPrefix(fdn);
		lines.add("SET");
		lines.add("(");
		lines.add(" mo \"" + ldn + "\"");
		lines.add(" nrOfAttributes 1");
		lines.add(" lastModification Integer 3");
		lines.add(")");		
	
		lines.add("DELETE");
		lines.add("(");
		lines.add(" mo \"" + ldn + "\"");
		lines.add(" exception none");
		lines.add(")");		
	}
	
	public void createExternalENodeBFunction( String eenbfFdn, ExternalENodeBFunction eenbf, String[] plmnId ) {
		writeHeader(eenbfFdn, 5);

		String intType = "Integer";
		if (eenbfFdn.contains("dg2")){
			intType = "Int32";
		}
		lines.add(" eNodeBPlmnId Struct");
		lines.add("  nrOfElements 3");
		lines.add("   mcc "+intType+" " + plmnId[0]);
		lines.add("   mnc "+intType+" " + plmnId[1]);
		lines.add("   mncLength "+intType+" " + plmnId[2]);
		lines.add(" eNBId "+intType+" " + eenbf.targetEnbId);
		lines.add(" lastModification Integer 6");		
		lines.add(" createdBy Integer " + eenbf.createdBy);		
		lines.add(" timeOfCreation String \"" + TimeOfCreation.getInstance().format( eenbf.lastUpdated) + "\"");

		lines.add(")");
	}
	
	public void createExternalEUtranCellFDD( String eeucFdn, ExternalEUtranCellFDD eeuc, EUtranCellFDD srcCell, 
			String eutranFrequencyRef, String[] plmnId, int lastModification  ) {
		writeHeader(eeucFdn, 12);

		String intType = "Integer";
		if (eeucFdn.contains("dg2")){
			intType = "Int32";
		}
		lines.add(" localCellId "+intType+" " + eeuc.localCellId);
		lines.add(" activePlmnList Array Struct 1");
		lines.add("  nrOfElements 3");
		lines.add("   mcc "+intType+" " + plmnId[0]);
		lines.add("   mnc "+intType+" " + plmnId[1]);
		lines.add("   mncLength "+intType+" " + plmnId[2]);
		lines.add(" physicalLayerCellIdGroup "+intType+" " + String.valueOf(srcCell.physicalLayerCellIdGroup));
		lines.add(" physicalLayerSubCellId "+intType+" " + String.valueOf(srcCell.physicalLayerSubCellId));
		lines.add(" tac "+intType+" " + String.valueOf(srcCell.tac));
		lines.add(" dlChannelBandwidth "+intType+" " + srcCell.dlChannelBandwidth);
		lines.add(" ulChannelBandwidth "+intType+" " + srcCell.ulChannelBandwidth);
		lines.add(" eutranFrequencyRef Ref \"" + stripPrefix(eutranFrequencyRef) + "\"");
		lines.add(" lastModification Integer " + lastModification);
		lines.add(" createdBy Integer " + eeuc.createdBy);		
		lines.add(" timeOfCreation String \"" + TimeOfCreation.getInstance().format(eeuc.lastUpdated)  + "\"");
		lines.add(" isRemoveAllowed Boolean true");

		lines.add(")");
		
	}
	
	public void createEUtranCellRelation( EUtranCellRelation eucr, String fdn, String  neighborCellRef) {
		writeHeader(fdn,5);
		
		lines.add(" neighborCellRef Ref \"" + stripPrefix(neighborCellRef) + "\"");
		
		lines.add(" lastModification Integer 6");
		lines.add(" createdBy Integer " + eucr.createdBy);		
		lines.add(" timeOfCreation String \"" + TimeOfCreation.getInstance().format(eucr.timeOfCreation) + "\"");
		lines.add(" isRemoveAllowed Boolean true");

		lines.add(")");
	}

	public void createTermPointToENB(String tptENBfdn, Date timeOfCreation,
			String ipAddress) {
		writeHeader(tptENBfdn,3);
/*
		String ipv4Addr = "";
		String ipv6Addr = "::";
/*		Disable to see if this stops the phantom TermPointToENB creations
 		if ( ipAddress.indexOf(":") == -1 ) {
			ipv4Addr = ipAddress;
		} else {
			ipv6Addr = ipAddress;
		}
*/		
		/* Disabling these atrributes as we are not setting anything on these
		lines.add(" ipAddress String \"" + ipv4Addr + "\"");
		lines.add(" usedIpAddress String \"" + ipv4Addr + "\"");
		lines.add(" ipv6Address String \"" + ipv6Addr + "\"");
		*/
		lines.add(" lastModification Integer 6");
		lines.add(" createdBy Integer " + CreatedByEutran.X2);		
		lines.add(" timeOfCreation String \"" + TimeOfCreation.getInstance().format(timeOfCreation) + "\"");
		lines.add(")");		
	}

	public void setMoAttribute(String fdn, int numAttributes, Date timeOfLastModification, List<String> attrLines) {
		String ldn = stripPrefix(fdn);
		lines.add("SET");
		lines.add("(");
		lines.add(" mo \"" + ldn + "\"");
		lines.add(" nrOfAttributes " + String.valueOf(2+numAttributes));
		lines.add(" lastModification Integer 0");
		lines.add(" timeOfLastModification String \"" + TimeOfCreation.getInstance().format(timeOfLastModification) + "\"");

		for ( String attrLine : attrLines ) {
			lines.add(attrLine);
		}
		
		lines.add(")");				
	}
	
	public boolean hasContent() {
		return lines.size() > 0;
	}
	
	public void dump( PrintWriter kerOut ) throws Exception {
		for ( String line : lines ) {
			kerOut.println(line);
			logger.finer(line);
		}
		lines.clear();
	}
	
	private void writeHeader(String fdn, int numAttributes) {
		String ldn = stripPrefix(fdn);
		
		int rdnStart = ldn.lastIndexOf(",") + 1;
		String rdn = ldn.substring(rdnStart);
		String moTypeId[] = rdn.split("=");
		
		lines.add("CREATE");
		lines.add("(");
		lines.add(" parent \"" + ldn.substring(0,rdnStart-1) + "\"");
		lines.add(" identity \"" + moTypeId[1] + "\"");
		lines.add(" moType " + moTypeId[0]);
		lines.add(" exception none");
		lines.add(" nrOfAttributes " + numAttributes);
	}
	
	private String stripPrefix(String fdn) {
		return fdn.substring(fdn.indexOf("ManagedElement="));
	}
}
