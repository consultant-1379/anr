package com.ericsson.oss.anrx2.simulator.netsim.candidate;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.ericsson.oss.anrx2.simulator.db.CandidateRelatedCellData;
import com.ericsson.oss.anrx2.simulator.netsim.KertayleSession;
import com.ericsson.oss.anrx2.simulator.netsim.constants.NetsimConstants;


public class CandRelHelper {

	public static void setCandRel(String srcFreqRelFdn,
			List<CandidateRelatedCellData> cRelsData, Date timeOfLastModification, String plmnId[], KertayleSession sess, String neMIMVersion) {

		//Checking for mim version of the node such that we can create the mobility status reason element only for 16A Version onwards
		boolean isMIMchangedforMobilityStatus=IdentifyMimChageformobilityStatus(srcFreqRelFdn,neMIMVersion,NetsimConstants.mobilityElemChangeVerERBS,NetsimConstants.mobilityElemChangeVerDG2);
		
		List<String> lines = new LinkedList<String>();
		lines.add(" candNeighborRel Array Struct " + cRelsData.size());
		for ( CandidateRelatedCellData cRelData : cRelsData ) {

			String intType = "Integer";
			if (srcFreqRelFdn.contains("dg2")){
				intType = "Int32";
			}

			if(isMIMchangedforMobilityStatus)
				lines.add("  nrOfElements 9");
			else
				lines.add("  nrOfElements 8");

			lines.add("   physicalLayerSubCellId "+intType+" " + cRelData.physicalLayerSubCellId);
			lines.add("   physicalLayerCellIdGroup "+intType+" " + cRelData.physicalLayerCellIdGroup);
			lines.add("   cellId "+intType+" " + cRelData.cellId);
			lines.add("   enbId "+intType+" " + cRelData.enbId);
			lines.add("   mcc "+intType+" " + plmnId[0]);
			lines.add("   mnc "+intType+" " + plmnId[1]);
			lines.add("   mncLength "+intType+" " + plmnId[2]);
			lines.add("   tac "+intType+" " + cRelData.tac);

			if(isMIMchangedforMobilityStatus)
				lines.add("   mobilityStatusReason Integer 0");

			lines.add("");
		}

		sess.setMoAttribute(srcFreqRelFdn, 1, timeOfLastModification, lines);
	}

	public static boolean IdentifyMimChageformobilityStatus(String srcFreqRelFdn, String enBMimVer,
			String mobilityElemChangeVerERBS, String mobilityElemChangeVerDG2) {
		if(srcFreqRelFdn.contains("dg2")){

			int intMimVer, intBaseVer;
			intMimVer = Integer.valueOf(enBMimVer.substring(0,2));
			intBaseVer = Integer.valueOf(mobilityElemChangeVerDG2.substring(0,2));
			if (intMimVer>intBaseVer) {return true;}
			else if (intMimVer<intBaseVer) {return false;}
			else{
				Character charofMimVer = enBMimVer.charAt(2);
				Character charofmobilityElemChangeVer = mobilityElemChangeVerDG2.charAt(2);
				int compare = charofMimVer.compareTo(charofmobilityElemChangeVer);
				if (compare >= 0)
					return true;
				else return false;
			}
		}
		else{
		enBMimVer = enBMimVer.replaceAll("[.]", "");
		mobilityElemChangeVerERBS = mobilityElemChangeVerERBS.replaceAll("[.]", "");
		// In OSSRC we will get mim version as vF.1.1.0 where as in ENM we will
		// get F.1.1.0
		if (enBMimVer.substring(0, 1).equals("v"))
			enBMimVer = enBMimVer.substring(1);

		Character charofMimVer = enBMimVer.charAt(0);
		Character charofmobilityElemChangeVer = mobilityElemChangeVerERBS.charAt(0);

		int compare = charofMimVer.compareTo(charofmobilityElemChangeVer);

		if (compare > 0)
			return true;
		else if (compare == 0) {
			int intMimVer, intBaseVer;
			intMimVer = Integer.valueOf(enBMimVer.substring(1));
			intBaseVer = Integer.valueOf(mobilityElemChangeVerERBS.substring(1));
			if (intMimVer >= intBaseVer)
				return true;
			else
				return false;
		}

		else
			return false;
	}
	}

}
