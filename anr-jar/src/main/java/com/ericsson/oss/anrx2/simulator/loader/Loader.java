package com.ericsson.oss.anrx2.simulator.loader;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.db.ENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.EUtranCellRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFreqRelation;
import com.ericsson.oss.anrx2.simulator.db.EUtranFrequency;
import com.ericsson.oss.anrx2.simulator.db.ExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.ExternalEUtranCellFDD;
import com.ericsson.oss.anrx2.simulator.db.IStorable;
import com.ericsson.oss.anrx2.simulator.db.OperExternalENodeBFunction;
import com.ericsson.oss.anrx2.simulator.db.TermPointToENB;
import com.ericsson.oss.anrx2.simulator.db.TimeOfCreation;
import com.ericsson.oss.anrx2.simulator.engine.CreatedByEutran;
import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;

public class Loader {
	private static final int DEFAULT_DIST = 10000;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			new Loader().run(args);
		} catch ( Throwable t ) {
			t.printStackTrace();
			System.exit(1);
		}

		System.exit(0);
	}

	private void run(String[] args) throws Exception {		
		Db db = Db.getInstance();
		load(args[0], db);
	}

	private void load(String directory, Db db) throws Exception {
		// MeContexts and ENodeBFunctions
		log("Parse MeContexts and ENodeBFunctions");
		Map<String,Object[]> nodeData = loadMeContexts(directory);
		Map<String,Object[]> managedElementData = loadManagedElement(directory);
		Map<String,Object[]> nodeDataENM = null;
		Map<String,Integer> eNBId = loadENodeBFunctions(directory);
		log("INFO: Read " + nodeData.size() + " MeContexts and " + eNBId.size() + " ENodeBFunctions");
		
		if(nodeData.isEmpty()){
			nodeDataENM = loadNetworkElementENM(directory);
		}
		Map<String,ENodeBFunction> enbFunc = new HashMap<String,ENodeBFunction>();
		Map<Integer,ENodeBFunction> enbFuncById = new HashMap<Integer,ENodeBFunction>();

		for ( Map.Entry<String, Integer> enbIdEntry : eNBId.entrySet() ) {
			String eNBFuncFdn = enbIdEntry.getKey();
			String meContextFdn = eNBFuncFdn.substring(0, eNBFuncFdn.indexOf(",ManagedElement="));
			Object[] meCon = nodeData.get(meContextFdn);

			String ipAddress = "";
			String neMIMVersion;

			if ( meCon != null ) {
				if ( ((Integer)meCon[1]).intValue() != 2 ||  ((Integer)meCon[2]).intValue() != 3 ) {
					log("WARN: Node not sync'd " + eNBFuncFdn);
				}
				ipAddress = (String)meCon[0];

				if (meContextFdn.contains("dg2")){
					Object[] ManaEle = managedElementData.get(meContextFdn);
					neMIMVersion=(String)ManaEle[1];
				}
				else {
					neMIMVersion=(String)meCon[3];
				}
			}
			else {
				Object[] neEle = nodeDataENM.get(meContextFdn);
				neMIMVersion=(String)neEle[1];
			}
					
			String meConId = meContextFdn.substring(meContextFdn.lastIndexOf("=")+1);
			String sim = "";
			if (meConId.contains("dg2") && (meConId.indexOf("dg2ERBS") != -1 )){
				sim = meConId.substring(0,meConId.indexOf("dg2ERBS"));
			}
			else {
				if ( meConId.indexOf("ERBS") != -1 ) {
					sim = meConId.substring(0,meConId.indexOf("ERBS"));
				}
			}
			// ENM has the netsim host in MeContext id 
			if ( sim.indexOf("_") != -1 ) {
				sim = sim.substring(sim.indexOf("_") +1);
			}
			
			ENodeBFunction anEnbFunc = new ENodeBFunction(eNBFuncFdn,enbIdEntry.getValue(),ipAddress,sim,neMIMVersion);
			enbFunc.put(eNBFuncFdn, anEnbFunc );
			
			if ( enbFuncById.containsKey(enbIdEntry.getValue()) ) {
				log("WARN: Duplication eNBId: " + enbIdEntry.getValue());
				log("   " + eNBFuncFdn);
				log("   " + enbFuncById.get(enbIdEntry.getValue()).fdn);
			}
			
			enbFuncById.put(enbIdEntry.getValue(),anEnbFunc);
		}

		// ExternalENodeBFunction
		log("Parse ExternalENodeBFunctions");
		Map<String,Object[]> externEnbFunc = loadExternalENodeBFunction(directory);
		List<IStorable> anrX2enbFunc = new LinkedList<IStorable>();
		Map<String,ExternalENodeBFunction> anrX2enbFuncByFdn = new HashMap<String,ExternalENodeBFunction>();
		List<IStorable> operatorEENB = new LinkedList<IStorable>();
		
		int missingENodeBFunction = 0;
		int discardedIllFormedEENBId = 0;
		for ( Map.Entry<String, Object[]> entry : externEnbFunc.entrySet() ) {
			String fdn = entry.getKey();
			Object[] params = entry.getValue();
			int createdBy = ((Integer)params[1]).intValue();
			String timeOfCreationStr = (String)params[2];
			
			ENodeBFunction thisEnbFunc = getENB(fdn,",EUtraNetwork",enbFunc);
			if ( thisEnbFunc == null ) {
				throw new Exception("ERROR: Cannot find ENodeBFunction containing " + fdn);
			}
			ENodeBFunction masterEnbFunc = enbFuncById.get(params[0]);
			if ( masterEnbFunc != null ) {
				String rdnId = fdn.substring(fdn.lastIndexOf("=")+1);
				String[] rdnIdParts = rdnId.split("-");
				boolean rdnIdOkay = (rdnIdParts.length == 2);				
				
				// if the proxy was created by ANR or X2
				if ( (createdBy == CreatedByEutran.ANR || createdBy == CreatedByEutran.X2) && rdnIdOkay ) {
					Date timeOfCreation = null;
					if ( timeOfCreationStr.length() > 0 ) {
						timeOfCreation = TimeOfCreation.getInstance().parse(timeOfCreationStr);
					} else {
						timeOfCreation = new Date();
					}
					ExternalENodeBFunction eenbf = new ExternalENodeBFunction(thisEnbFunc.eNBId, masterEnbFunc.eNBId, createdBy, timeOfCreation);
					anrX2enbFunc.add(eenbf);
					anrX2enbFuncByFdn.put(fdn, eenbf);
				} else {				
					if ( ! rdnIdOkay ) {
						discardedIllFormedEENBId++; 
					}
					thisEnbFunc.operExternEnbFunc++;
					operatorEENB.add(new OperExternalENodeBFunction(thisEnbFunc.eNBId,masterEnbFunc.eNBId));
				}
			} else {
				log("WARN: Cannot find master ENodeBFunction for " + params[0] + ": " + fdn);
				missingENodeBFunction++;
			}
		}
		if ( discardedIllFormedEENBId > 0 ) {
			log("WARN: Reclassified " + discardedIllFormedEENBId + " ExternalENodeBFunction as the RDN ID was ill-formed");
		}
		if ( missingENodeBFunction > 0 ) {
			log("WRAN: Missing ENodeBFunction: " + missingENodeBFunction);
		}

		
		// TermPointToENB
		log("Parse TermPointToENB");
		int discardedIllFormedTPENBId = 0;
		Map<String,int[]> termPointToENB = loadTermPointToENB(directory);
		List<IStorable> anrX2_TPENB = new LinkedList<IStorable>();		
		for ( Map.Entry<String, int[]> entry : termPointToENB.entrySet() ) {
			String fdn = entry.getKey();
			int[] params = entry.getValue();
			int createdBy = params[0];

			if (createdBy == CreatedByEutran.ANR || createdBy == CreatedByEutran.X2) {
				String rdnId = fdn.substring(fdn.lastIndexOf("=")+1);
				String[] rdnIdParts = rdnId.split("-");
				boolean rdnIdOkay = (rdnIdParts.length == 2);				
				if ( rdnIdOkay ) {			
					String eenbfFdn = fdn.substring(0, fdn.lastIndexOf(","));
					ExternalENodeBFunction eenbf = anrX2enbFuncByFdn.get(eenbfFdn);
					if ( eenbf == null ) {
						throw new Exception("WARN: Cannot find ANR/X2 ExternalNodeBFunction for " + fdn);
					}
							
					TermPointToENB tptenb = new TermPointToENB(eenbf.ownerEnbId, eenbf.targetEnbId, createdBy);
					anrX2_TPENB.add(tptenb);
				} else {				
					discardedIllFormedTPENBId++; 
				}
			} 
		}
		if ( discardedIllFormedTPENBId > 0 ) {
			log("WARN: Reclassified " + discardedIllFormedTPENBId + " TermPointToENB as the RDN ID was ill-formed");
		}
		
		
		// EUtranCellFDD
		log("Parse EUtranCellFDD");		
		Map<String,int[]> eucellfddData = loadEUtranCellFDD(directory);
		List<IStorable> eucellList = new ArrayList<IStorable>(eucellfddData.size());
		Map<String,EUtranCellFDD> eucellByFdn= new HashMap<String,EUtranCellFDD>();
		for ( Map.Entry<String, int[]> entry : eucellfddData.entrySet() ) {
			String fdn = entry.getKey();
			int[] params = entry.getValue();
			ENodeBFunction thisEnbFunc = getENB(fdn,",EUtranCellFDD",enbFunc);
			if ( thisEnbFunc == null ) {
				throw new Exception("ERROR: Cannot find ENodeBFunction containing " + fdn);
			}
			EUtranCellFDD cell = new EUtranCellFDD(thisEnbFunc.eNBId,params[0],params[1],params[2],params[3],
					params[4],params[5],params[6],params[7],new CellLocation(params[8],params[9],params[10]),fdn);
			eucellList.add(cell);
			eucellByFdn.put(cell.fdn,cell);
		}
		log("INFO: Found " + eucellList.size() + " EUtranCellFDD");
		
		// ExternalEUtranCellFDD
		log("Parse ExternalEUtranCellFDD");		
		Map<String,Object[]> eeucellData = loadExternalEUtranCellFDD(directory);
		Map<String,ExternalEUtranCellFDD> eeucellByFdn= new HashMap<String,ExternalEUtranCellFDD>();
		int operCreated = 0;
		int missingExternalENodeBFunction = 0;	
		for ( Map.Entry<String, Object[]> entry : eeucellData.entrySet() ) {
			String fdn = entry.getKey();
			Object[] params = entry.getValue();
			int createdBy = ((Integer)params[0]).intValue();
			int cellId = ((Integer)params[1]).intValue();
			String timeOfCreationStr = (String)params[2]; 
			
			// Only interested in ANR/X2
			// Due to bug in simulations also check that the timeOfCreation has been set			
			if ( (createdBy == CreatedByEutran.ANR || createdBy == CreatedByEutran.X2) &&
					timeOfCreationStr.length() > 0 ) {
				String eenbfFdn = fdn.substring(0, fdn.lastIndexOf(","));
				ExternalENodeBFunction eenbf = anrX2enbFuncByFdn.get(eenbfFdn);
				if ( eenbf == null ) {
					log("WARN: Cannot find ANR/X2 ExternalNodeBFunction for " + fdn);
					missingExternalENodeBFunction++;
				} else {					
					Date timeOfCreation = TimeOfCreation.getInstance().parse(timeOfCreationStr);
					ExternalEUtranCellFDD eeucell = new ExternalEUtranCellFDD(eenbf.ownerEnbId,eenbf.targetEnbId,cellId, createdBy, timeOfCreation);
					eeucellByFdn.put(fdn,eeucell);					
				}
			} else {
				operCreated++;
			}	
		}
		log(" Disgarded " + operCreated + " operator created ExternalEUtranCellFDD out of total of " + eeucellData.size());
		if ( missingExternalENodeBFunction > 0 ) {
			log("WARN: Disgarded " + missingExternalENodeBFunction + " ExternalEUtranCellFDD as the ExternalENodeBFunction was missing");
		}
		
		// EUtranFrequency
		log("Parse EUtranFrequency");				
		Map<String,Integer> eUFreqData = loadEUtranFrequencys(directory);
		List<IStorable> eUFreqList = new LinkedList<IStorable>();
		Map<String,EUtranFrequency> eUFreqByFDN = new HashMap<String,EUtranFrequency>();
		for ( Map.Entry<String, Integer> entry : eUFreqData.entrySet() ) {
			String fdn = entry.getKey();
			Integer arfcnValueEUtranDl = entry.getValue();
			ENodeBFunction thisEnbFunc = getENB(fdn,",EUtraNetwork",enbFunc);
			if ( thisEnbFunc == null ) {
				throw new Exception("ERROR: Cannot find ENodeBFunction containing " + fdn);
			} 
			EUtranFrequency euf = new EUtranFrequency(thisEnbFunc.eNBId,arfcnValueEUtranDl.intValue(),fdn); 
			eUFreqList.add(euf);
			eUFreqByFDN.put(fdn, euf);
		}

		// EUtranFreqRelation
		log("Parse EUtranFreqRelation");				
		Map<String,Object[]> eUFreqRelData = loadEUtranFreqRelations(directory);		
		List<IStorable> eUFreqRelList = new ArrayList<IStorable>(eUFreqRelData.size());
		Map<String,EUtranFreqRelation> eUFreqRelByFdn = new HashMap<String,EUtranFreqRelation>();
		List<int[]> candidateRels = new LinkedList<int[]>();
		int numUnsupportedCellType = 0;
		for ( Map.Entry<String, Object[]> entry : eUFreqRelData.entrySet() ) {
			String fdn = entry.getKey();
			Object[] params = entry.getValue();

			EUtranFrequency euf =  eUFreqByFDN.get((String)params[1]);
			if ( euf == null ) {
				throw new Exception("ERROR: Cannot find EUtranFrequency for " + fdn);					
			}
			String cellFdn = fdn.substring(0,fdn.lastIndexOf(","));
			EUtranCellFDD cell = eucellByFdn.get(cellFdn);
			if ( cell == null ) {
				String cellRdn = cellFdn.substring(0,cellFdn.lastIndexOf(","));			
				if ( cellRdn.startsWith("EUtranCellFDD") ) {
					throw new Exception("ERROR: Cannot find EUtranCellFDD for " + cellFdn);
				} else {
					//log("WARN: Skipping EUtranFreqRelation for unsupported Cell Type " + fdn);
					numUnsupportedCellType++;
				}
			} else {
				String rdnId = fdn.substring(fdn.lastIndexOf("=")+1);
				int createdBy = ((Integer)params[0]).intValue();
				EUtranFreqRelation eufr = new EUtranFreqRelation(cell.enbId,cell.cellId,euf.arfcnValueEUtranDl,createdBy,rdnId);
				eUFreqRelList.add(eufr);
				eUFreqRelByFdn.put(fdn,eufr);
				
				if ( params[2] != null) {
					if (!(params[2].toString().contains("0;0;0;0;0;0;0;0"))){
						for ( String candRel : ((String)params[2]).split(":") ) {
							String[] candRelParam = candRel.split(";");
							Integer cellIdB = Integer.parseInt(candRelParam[2]);
							Integer enbIdB = Integer.parseInt(candRelParam[3]);
							candidateRels.add(new int[] { cell.enbId, cell.cellId, enbIdB, cellIdB, euf.arfcnValueEUtranDl });
						}
					}
				}				
			}
		}
		if ( numUnsupportedCellType > 0 ) {
			log("WARN: Disgarded " + numUnsupportedCellType + " EUtranFreqRelation due to unsupported cell type");
		}
					
		log("Parse EUtranCellRelation");				
		Map<String,Object[]> eucrData = loadEUtranCellRelation(directory);
		log("INFO: Read " + eucrData.size() + " EUtranCellRelation");
		List<IStorable> eurelList = new ArrayList<IStorable>();
		int disgardedRelMissingEENBF = 0;
		int disgardedRelUnsupportedType = 0;
		for ( Map.Entry<String, Object[]> entry : eucrData.entrySet() ) {
			String fdn = entry.getKey();			
			Object[] params = entry.getValue();
			String neighborCellRef = (String)params[0];			
			int createdBy = ((Integer)params[1]).intValue();
			String timeOfCreationStr = (String)params[2];
			
			String rdnId = fdn.substring(fdn.lastIndexOf("=")+1);
			String[] rdnIdParts = rdnId.split("-");
			boolean rdnIdOkay = (rdnIdParts.length == 3);
			
			String freqRelFdn = fdn.substring(0,fdn.lastIndexOf(","));
			EUtranFreqRelation eufr = eUFreqRelByFdn.get(freqRelFdn);
			if ( eufr != null ) {		
				String cellFdn = freqRelFdn.substring(0,freqRelFdn.lastIndexOf(","));
				EUtranCellFDD cell = eucellByFdn.get(cellFdn);
				if ( cell == null ) {
					throw new Exception("Cannot find EUtranCellFDD for " + fdn);
				}
				if ( (createdBy == CreatedByEutran.ANR || createdBy == CreatedByEutran.X2) &&
						rdnIdOkay ) {
					Date timeOfCreation = null; 
					if ( timeOfCreationStr.length() > 0 ) {
					   timeOfCreation = TimeOfCreation.getInstance().parse(((String)params[2]));
					} else {
						log("WARN: Invalid value for timeOfCreation in " + fdn);
						timeOfCreation = new Date();
					}
					ExternalEUtranCellFDD eeucell = eeucellByFdn.get(neighborCellRef);
					if ( eeucell == null ) {
						// Can be an internal relation due to simulation fault
						if ( neighborCellRef.indexOf("ExternalEUtranCellFDD") != -1 ) {
							log("WARN: Cannot find ExternalEUtranCellFDD for " + neighborCellRef + " in " + fdn);
							disgardedRelMissingEENBF++;
						} else {
							cell.operRelations++;
							eufr.operRelations++;
						}
					} else {							
						EUtranCellRelation eurel = new EUtranCellRelation(eufr.enbId,eufr.cellId,eeucell.targetEnbId,eeucell.localCellId,
								eufr.arfcnValueEUtranDl,DEFAULT_DIST, createdBy,timeOfCreation);
						eurelList.add(eurel);
						// Increment the refCount for the ExternalEUtranCellFDD that this rel is pointing at
					}				
				} else {
					cell.operRelations++;
					eufr.operRelations++;
				}
			} else {
				if ( freqRelFdn.indexOf(",EUtranCellFDD=") != -1 ) {
					throw new Exception("Cannot find EUtranFreqRelation for " + fdn);
				} else {
					disgardedRelUnsupportedType++;
				}
			}
		}
		if ( disgardedRelMissingEENBF > 0 ) {
			log("WARN: Disgarded "+ disgardedRelMissingEENBF + " EUtranCellRelation because the ExternalEUtranCellFDD could not be found");
		}
		if ( disgardedRelUnsupportedType > 0 ) {
			log("WARN: Disgarded "+ disgardedRelUnsupportedType + " EUtranCellRelation because the cell type is unsupported");
		}
		log("INFO: Found " + eurelList.size() + " EUtranCellRelations");
		
		
		// Store phase
		log("Store ENodeBFunctions");
		db.loadStorable(new ArrayList<IStorable>(enbFunc.values()));		
		log("Store EUtranFrequency");
		db.loadStorable(eUFreqList);
		log("Store EUtranCellFDD");
		db.loadStorable(eucellList);		
		log("Store EUtranFreqRelation");
		db.loadStorable(eUFreqRelList);
		log("Store Operator ExternalENodeBFunctions");
		db.loadStorable(operatorEENB);

		log("Store ANR/X2 ExternalENodeBFunctions: " + anrX2enbFunc.size());
		db.loadStorable(anrX2enbFunc);
		log("Store ANR/X2 TermPointToENB: " + anrX2_TPENB.size());
		db.loadStorable(anrX2_TPENB);
		
		ArrayList<IStorable> anrX2eeucell = new ArrayList<IStorable>(eeucellByFdn.values());
		log("Store ANR/X2 ExternalEUtranCellFDD: " + anrX2eeucell.size());
		db.loadStorable(anrX2eeucell);
		log("Store ANR/X2 EUtranCellRelation: " + eurelList.size());		
		db.loadStorable(eurelList);
		
		if ( candidateRels.size() > 0 ) {
			ILoaderHelper cRelHepler = (ILoaderHelper)Class.forName(Loader.class.getPackage().getName() + ".helper.CandidateRel").newInstance();
			cRelHepler.processData(candidateRels, db);
		}
	}

	private Map<String,Object[]> loadMeContexts(String directory) throws Exception {
		Map<String,Object[]> nodeIpAddress = new HashMap<String,Object[]>();

		File meConFile = new File(directory + "/MeContext.modata");
		if ( ! meConFile.exists() ) {
			log("MeContext file doesn't exist");
			return nodeIpAddress;
		}
		
		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/MeContext.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 5 ) {
					nodeIpAddress.put(parts[0],new Object[] { parts[1], Integer.decode(parts[2]),Integer.decode(parts[3]), parts[4]});
				} else {
					throw new Exception("Failed to parse MeContext line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return nodeIpAddress;
}

	private Map<String,Object[]> loadNetworkElementENM(String directory) throws Exception {
		Map<String,Object[]> networkElement = new HashMap<String,Object[]>();

		File meConFile = new File(directory + "/NetworkElement.modata");
		if ( ! meConFile.exists() ) {
			log("NetworkElement file doesn't exist");
			return networkElement;
		}

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/NetworkElement.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 3 ) {
					networkElement.put(parts[1],new Object[] { parts[0], parts[2]});
				} else {
					throw new Exception("Failed to parse NetworkElement line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return networkElement;
}
	private Map<String,Object[]> loadManagedElement(String directory) throws Exception {
		Map<String,Object[]> ManagedElement = new HashMap<String,Object[]>();

		File meConFile = new File(directory + "/ManagedElement.modata");
		if ( ! meConFile.exists() ) {
			log("ManagedElement file doesn't exist");
			return ManagedElement;
		}

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/ManagedElement.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 3 ) {
					ManagedElement.put(parts[1],new Object[] { parts[0], parts[2]});
				} else {
					throw new Exception("Failed to parse ManagedElement line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return ManagedElement;
	}
	private Map<String,Integer> loadENodeBFunctions(String directory) throws Exception {
		Map<String,Integer> eNBId = new HashMap<String,Integer>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/ENodeBFunction.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 2 ) {
					eNBId.put(parts[0],Integer.valueOf(parts[1]));
				} else {
					throw new Exception("Failed to parse ENodeBFunction line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return eNBId;		
	}

	private Map<String,Object[]> loadExternalENodeBFunction(String directory) throws Exception {
		Map<String,Object[]> result = new HashMap<String,Object[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/ExternalENodeBFunction.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@",-1);
				if ( parts.length == 4 ) {
					if ( parts[3].equals("null") ) {
						parts[3] = "";
					}
					result.put(parts[0],new Object[] { Integer.valueOf(parts[1]), getCreatedBy(parts[2]),parts[3] });
				} else {
					throw new Exception("Failed to parse ExternalENodeBFunction line:" + in.getLineNumber() + " \"" + line + "\", #parts=" +parts.length);
				}
			}
		}
		return result;		
	}

	private Map<String,int[]> loadTermPointToENB(String directory) throws Exception {
		Map<String,int[]> result = new HashMap<String,int[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/TermPointToENB.modata")) ) {
			in.readLine();			
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@",-1);
				if ( parts.length == 2) {
					int values[] = new int[1];
					for ( int index = 0; index < values.length; index++ ) {
						values[index] = getCreatedBy(parts[index+1]);
					}
					result.put(parts[0],values);
				} else {
					throw new Exception("Failed to parse TermPointToENB line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;		
	}
	
	private Map<String,int[]> loadEUtranCellFDD(String directory) throws Exception {
		Map<String,int[]> result = new HashMap<String,int[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/EUtranCellFDD.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 12 ) {
					int values[] = new int[11];
					for ( int index = 0; index < values.length; index++ ) {
						values[index] = Integer.parseInt(parts[index+1]);
					}
					result.put(parts[0],values);
				} else {
					throw new Exception("Failed to parse EUtranCellFDD line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;
	}

	private Map<String,Integer> loadEUtranFrequencys(String directory) throws Exception {
		Map<String,Integer> result = new HashMap<String,Integer>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/EUtranFrequency.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length == 2 ) {
					result.put(parts[0],Integer.valueOf(parts[1]));
				} else {
					throw new Exception("Failed to parse EUtranFrequency line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;		
	}
	
	private Map<String,Object[]> loadEUtranFreqRelations(String directory) throws Exception {
		Map<String,Object[]> result = new HashMap<String,Object[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/EUtranFreqRelation.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@");
				if ( parts.length >= 3 ) {
					String candidateRels = null;
					if ( parts.length >= 4 ) {
						candidateRels = parts[3];
					}
					result.put(parts[0],new Object[] { getCreatedBy(parts[1]),parts[2],candidateRels });
				} else {
					throw new Exception("Failed to parse EUtranFreqRelation, num parts=" + parts.length + ", line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;		
	}

	private Map<String,Object[]> loadEUtranCellRelation(String directory) throws Exception {
		Map<String,Object[]> result = new HashMap<String,Object[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/EUtranCellRelation.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@",-1);
				if ( parts.length == 4 ) {
					if ( parts[3].equals("null")) {
						parts[3] = "";
					}				
					result.put(parts[0],new Object[] { parts[1], getCreatedBy(parts[2]), parts[3] });
				} else {
					throw new Exception("Failed to parse EUtranCellRelation line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;		
	}

	private Map<String,Object[]> loadExternalEUtranCellFDD(String directory) throws Exception {
		Map<String,Object[]> result = new HashMap<String,Object[]>();

		try ( LineNumberReader in = new LineNumberReader(new FileReader(directory + "/ExternalEUtranCellFDD.modata")) ) {
			in.readLine();
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] parts = line.split("@",-1);
				if ( parts.length == 4 ) {
					if ( parts[3].equals("null")) {
						parts[3] = "";
					}
					result.put(parts[0], new Object[] { getCreatedBy(parts[1]), Integer.valueOf(parts[2]), parts[3] } ); 
				} else {
					throw new Exception("Failed to parse ExternalEUtranCellFDD line:" + in.getLineNumber() + " \"" + line + "\"");
				}
			}
		}
		return result;
	}

	
	private ENodeBFunction getENB( String fdn, String search, Map<String,ENodeBFunction> enbFunc ) {
		int endOfEnbFunc = fdn.indexOf(search);
		if ( endOfEnbFunc > - 1) {
			String thisEnbFuncFdn = fdn.substring( 0, endOfEnbFunc );
			ENodeBFunction thisEnbFunc = enbFunc.get(thisEnbFuncFdn);
			return thisEnbFunc;
		} else {
			return null;
		}	
	}
	
	private Integer getCreatedBy(String createdByStr) {
		if ( createdByStr.length() == 0 ) {
			return new Integer(0);
		} else {
			return Integer.parseInt(createdByStr);
		}
	}
	
	public static void log(String msg) {		
		System.out.println(new Date() + " " + msg);
	}
}
