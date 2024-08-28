package com.ericsson.oss.anrx2.xtractor.ossrc;

import com.versant.fund.*;

import java.util.*;
import java.io.*;

public class ExtractData {

	public static void main(String args[]) {
		try {
			new ExtractData().run(args);
		} catch ( Throwable t ) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	private void run(String args[]) throws Exception {
		boolean hasCandidateRelations = Boolean.getBoolean("hasCandidateRelations");
		
		FundSession session = new FundSession ("WRAN_SUBNETWORK_MIRROR_CS");       
		try {
			session.defaultLockMode(Constants.NOLOCK);

			ClassHandle cls = session.locateClass("com.ericsson.nms.cif.cs.ConfigurationImpl");
			Predicate isValid = session.newAttrBoolean("isValid").eq(true);

			Handle h_validCfg[] = cls.select(isValid).asArray();
			if ( h_validCfg.length != 1 )
				throw new IllegalStateException("Found invalid number of ValidConfiguration:" + h_validCfg.length );

			Predicate validCfg = session.newAttrHandle("moConfiguration").eq(h_validCfg[0]);

			Predicate isERBS = session.newAttrInt("neType").eq(4);
			Predicate isDG2 = session.newAttrInt("neType").eq(45);

			//String[] databaseClasses;
			String[] databaseClassMeContextERBS = {"ERBS_NODE_MODEL"};
			String[] databaseClassMeContextDG2 = {"ECIM_Top"};
			String[] databaseClasses = {"ERBS_NODE_MODEL", "Lrat"};
			getMOs( session, validCfg.and(isERBS), "MeContext", new Attr[] { session.newAttrString("ipAddress"),
				session.newAttrInt("connectionStatus"),session.newAttrInt("mirrorMIBsynchStatus"), session.newAttrString("neMIMversion") }, args[0], databaseClassMeContextERBS, false);

			getMOs( session, validCfg.and(isDG2), "MeContext", new Attr[] { session.newAttrString("ipAddress"),
					session.newAttrInt("connectionStatus"),session.newAttrInt("mirrorMIBsynchStatus"), session.newAttrString("neMIMversion") }, args[0], databaseClassMeContextDG2, true);

			getMOs( session, validCfg, "ManagedElement", new Attr[] { session.newAttrString("dnPrefix"),
				session.newAttrString("release") }, args[0], databaseClassMeContextDG2, false);

			getMOs( session, validCfg, "ENodeBFunction", new Attr[] {  session.newAttrInt("eNBId") }, args[0], databaseClasses, false);
			getMOs( session, validCfg, "EUtranCellFDD", 
					new Attr[] { 
						session.newAttrInt("cellId"), session.newAttrInt("tac"),
						session.newAttrInt("physicalLayerCellIdGroup"),
						session.newAttrInt("physicalLayerSubCellId"), 
						session.newAttrInt("ulChannelBandwidth"), session.newAttrInt("dlChannelBandwidth"),
						session.newAttrInt("earfcndl"),session.newAttrInt("earfcndl"),
						session.newAttrInt("latitude"), session.newAttrInt("longitude"), session.newAttrInt("altitude") 
			            },
				    args[0], databaseClasses, false);
			
			if ( hasCandidateRelations ) {
				getMOs( session, validCfg, "EUtranFreqRelation", 
						new Attr[] { 
						session.newAttrInt("createdBy"), 
						session.newAttrString("eutranFrequencyRef"),
						session.newAttrHandle("candNeighborRel")
				},
				args[0], databaseClasses, false);
			} else {
				getMOs( session, validCfg, "EUtranFreqRelation", 
						new Attr[] { 
						session.newAttrInt("createdBy"), 
						session.newAttrString("eutranFrequencyRef")
				},
				args[0], databaseClasses, false);
			}
			
			getMOs( session, validCfg, "EUtranFrequency", new Attr[] { session.newAttrInt("arfcnValueEUtranDl") }, args[0], databaseClasses, false );
			
			getMOs( session, validCfg, "ExternalENodeBFunction", 
					new Attr[] { session.newAttrInt("eNBId"), session.newAttrInt("createdBy"), session.newAttrString("timeOfCreation") },
					args[0], databaseClasses, false);

			getMOs( session, validCfg, "TermPointToENB", 
					new Attr[] { session.newAttrInt("createdBy") },
					args[0], databaseClasses, false);

			getMOs( session, validCfg, "ExternalEUtranCellFDD", 
					new Attr[] { session.newAttrInt("createdBy"), 
					             session.newAttrInt("localCellId"),
					             session.newAttrString("timeOfCreation")}, 
					args[0], databaseClasses, false);

			getMOs( session, validCfg, "EUtranCellRelation", new Attr[] { session.newAttrString("neighborCellRef"), session.newAttrInt("createdBy") , session.newAttrString("timeOfCreation")}, args[0], databaseClasses, false );
			
			log("Done");
		} finally {
			session.rollback();
			session.endSession();
		}

	}

	private void log(String msg) {
		System.out.println(new Date() + " "+ msg);
	}

	private void getMOs( FundSession session, Predicate inValid, String moType, Attr attrs[], String directory,String[] databaseClasses, boolean appendFile) throws Exception {
		log("Reading " + moType);

		HandleHelper[] helpers = new HandleHelper[attrs.length];
		for ( int attrIndex = 0; attrIndex < attrs.length; attrIndex++ ) {
			if ( attrs[attrIndex] instanceof AttrHandle ) {
				String attrName = attrs[attrIndex].name();						
		        String helperClassName = ExtractData.class.getPackage().getName() + ".helper." + 
		        		(Character.toUpperCase(attrName.charAt(0)) + attrName.substring(1));
		        try {
		        	@SuppressWarnings("rawtypes")
					Class helperClass = Class.forName(helperClassName);
		        	helpers[attrIndex] = (HandleHelper)helperClass.newInstance();
		        } catch ( ClassNotFoundException ignored ) {}
			}
		}


	//	try ( PrintWriter out = new PrintWriter(directory + "/" + moType + ".modata") ) {
		try ( PrintWriter out = new PrintWriter(new FileOutputStream(new File(directory + "/" + moType + ".modata"), appendFile)) ) {
		if (!appendFile){
				out.print("FDN");
				for ( Attr attr : attrs ) {
					out.print(":" + attr.name());
				}
				out.println();
			}

			AttrString moFDNAttr = session.newAttrString("moFDN");
			for(String databaseClass : databaseClasses){
				ClassHandle moCls = session.locateClass("com.ericsson.nms.cif.cs.mo.mim"+databaseClass+"." + moType + "Impl");
				HandleEnumeration e = moCls.select(inValid);
				while ( e.hasMoreHandles() ) {
					Handle batch[] = e.nextBatch(1000).asArray();
					for ( Handle mo : batch ) {
						try {
							StringBuffer line = new StringBuffer();
							line.append(getFDN(mo.get(moFDNAttr)));
							for ( int attrIndex = 0; attrIndex < attrs.length; attrIndex++ ) {
								line.append("@");
								if ( helpers[attrIndex] == null ) {
									line.append(mo.getObject(attrs[attrIndex]));
								} else {
									Handle attrValue = mo.get((AttrHandle)attrs[attrIndex]);
									if ( ! attrValue.isEmpty() ) {
										String attrStr = helpers[attrIndex].processHandle(attrValue, session);
										line.append(attrStr);
									}
								}
							}
							out.println(line.toString());
						} catch ( VException ve ) {
							if ( ve.getErrno() == 5006 ) {
								System.out.println("WARN: MO deleted while reading");
							} else {
								throw ve;
							}
						}
					}
				}
			}
		}
	}
    private String getFDN( String fdn ) {
        String result;
        if ( fdn.endsWith(",") ) 
            result = fdn.substring(0, fdn.length() - 1 );
        else if ( fdn.endsWith("!valid") ) 
            result = fdn.substring(0, fdn.length() - 6 );
        else
            result = fdn;

        return result;
    }		
}