package com.ericsson.oss.anrx2.xtractor.enm;

import java.io.PrintWriter;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;

import com.versant.jpa.generic.DatabaseClass;
import com.versant.jpa.generic.DatabaseField;
import com.versant.jpa.generic.DatabaseObject;


public class ExtractData {
	private static final String PERSISTENCE_UNIT_NAME_GENERIC = "dps_genericAccess";

	public static void main(String args[]) {
		try {
			new ExtractData().run(args);
		} catch ( Throwable t ) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	private void run(String args[]) throws Exception {
		final boolean hasCandidateRelations = Boolean.getBoolean("hasCandidateRelations");
		final EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME_GENERIC);
        final EntityManager em = emf.createEntityManager();

		try {
//			getMOs( em.and(isERBS), "MeContext", new String[] { session.newAttrString("ipAddress"), 
//				"connectionStatus"),"mirrorMIBsynchStatus") }, args[0] );

			// Reading ossPrefix and release to identify mim version of the Node
			getMOs( em, "NetworkElement", new String[] { "ossPrefix", "release" }, args[0]);

			getMOs( em, "ENodeBFunction", new String[] {  "eNBId" }, args[0]);
			
			getMOs( em, "EUtranCellFDD", 
					new String[] { 
						"cellId", "tac",
						"physicalLayerCellIdGroup",
						"physicalLayerSubCellId", 
						"ulChannelBandwidth", "dlChannelBandwidth",
						"earfcndl","earfcndl",
						"latitude", "longitude", "altitude"
			            },
				    args[0]);
			
			if ( hasCandidateRelations ) {
				getMOs( em, "EUtranFreqRelation", 
						new String[] {
						"createdBy", 
						"eutranFrequencyRef",						
						"candNeighborRel"						
				},
				args[0]);
			} else {
				getMOs( em, "EUtranFreqRelation", 
						new String[] { 
						"createdBy", 
						"eutranFrequencyRef"
				},
				args[0]);				
			}
			
			getMOs( em, "EUtranFrequency", new String[] { "arfcnValueEUtranDl" }, args[0] );
			
			getMOs( em, "ExternalENodeBFunction", 
					new String[] { "eNBId", "createdBy", "timeOfCreation" },
					args[0]);
			getMOs( em, "TermPointToENB", 
					new String[] { "createdBy" },
					args[0]);			
			getMOs( em, "ExternalEUtranCellFDD", 
					new String[] { "createdBy", 
					             "localCellId",
					             "timeOfCreation"}, 
					args[0]);		
			getMOs( em, "EUtranCellRelation", new String[] { "neighborCellRef", "createdBy", "timeOfCreation"}, args[0] );
			
			log("Done");
		} finally {
			em.close();
			emf.close();
		}

	}

	private void log(String msg) {
		System.out.println(new Date() + " "+ msg);
	}

	private void getMOs( final EntityManager em, final String moType, final String[] attrs, final String directory ) throws Exception {
		log("Reading " + moType);

		try ( PrintWriter out = new PrintWriter(directory + "/" + moType + ".modata") ) {
			out.print("FDN");
			for ( String attr : attrs ) {
				out.print(":" + attr);
			}
			out.println();
		
			final EntityTransaction transaction = em.getTransaction();
			transaction.begin();
			
			String dataBaseClass;
			if (moType.equals("NetworkElement"))
				dataBaseClass="ns_OSS_NE_DEF.Pt_";
			else
				dataBaseClass="ns_ERBS_NODE_MODEL.Pt_";

			//	final DatabaseClass clazz = DatabaseClass.forName("ns_ERBS_NODE_MODEL.Pt_" + moType, em.getEntityManagerFactory());
			final DatabaseClass clazz = DatabaseClass.forName(dataBaseClass + moType, em.getEntityManagerFactory());
			DatabaseField[] fields = new DatabaseField[attrs.length];			
			AttributeHelper[] helpers = new AttributeHelper[attrs.length];			
			for ( int attrIndex = 0; attrIndex < attrs.length; attrIndex++ ) {
				// Hack here. ENM doesn't have createdBy or timeOfCreation in the DB
				// So we'll fake it
				if ( attrs[attrIndex].equals("createdBy") || attrs[attrIndex].equals("timeOfCreation") ) {
					fields[attrIndex] = null;
				} else {
					fields[attrIndex] = clazz.getDeclaredField("at_" + attrs[attrIndex]);
				}

				if ( attrs[attrIndex].equals("candNeighborRel") ) {					
			        String helperClassName = ExtractData.class.getPackage().getName() + ".helper." + 
			        		(Character.toUpperCase(attrs[attrIndex].charAt(0)) + attrs[attrIndex].substring(1));
		        	//System.out.println("Creating helper for " + attrs[attrIndex] + ", creating " + helperClassName);
			        
			        try {
			        	@SuppressWarnings("rawtypes")
						Class helperClass = Class.forName(helperClassName);
			        	helpers[attrIndex] = (AttributeHelper)helperClass.newInstance();
			        } catch ( ClassNotFoundException ignored ) {}
					
				}
			}

			//	do a query, all returned objects, regardless of actual type are represented as "DatabaseObject"
		//	final String query = "select moe from ns_ERBS_NODE_MODEL.Pt_" + moType + " moe where moe.bucketName = 'Live'";
			final String query = "select moe from "+ dataBaseClass + moType + " moe where moe.bucketName = 'Live'";
			List<DatabaseObject> results = em.createQuery(query, DatabaseObject.class).setLockMode(LockModeType.NONE).getResultList();

			
			DatabaseField fdnField = clazz.getSuperclass().getDeclaredField("fdn");			
			for ( DatabaseObject result : results ) {
				StringBuffer line = new StringBuffer();
				line.append(fdnField.get(result));
				for ( int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++ ) {
					line.append("@");
					// Rest of hack for createdBy/timeOfCreation here, if fields[fieldIndex] is null don't write anything 
					if ( fields[fieldIndex] != null ) {
						Object value = fields[fieldIndex].get(result);
						if ( helpers[fieldIndex] == null ) {
							line.append(value);
						} else {
							if ( value != null ) {
								line.append(helpers[fieldIndex].processAttribute(value, em));	
							}						
						}
					}
				}
				out.println(line.toString());
			}
			
			transaction.rollback();
		}
	}	
}
