package com.ericsson.oss.anrx2.simulator.engine;

import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Config {
	private static Config inst;
	private String[] plmnId;
	
	private Properties props = new Properties();;
	public static Config getInstance() {
		if ( inst == null ) {
			inst = new Config();
		}
		return inst;
	}
	
	public Properties getProps() {
		return props;
	}
	
	private Config() {
		String propsFile = System.getProperty("simulator.engine.config");
		if ( propsFile == null ) {
			System.out.println("simulator.engine.config not defined");
			System.exit(1);
		}
		try {
			FileReader fr = new FileReader(propsFile);
			props.load(fr);
			fr.close();
		} catch ( Throwable t ) {
			t.printStackTrace();
			System.exit(1);
		} 
	}
	
	public String getManditoryParam(String paramName) throws IllegalStateException {
		String paramValue = props.getProperty(paramName);
		if ( paramValue == null ) {
			throw new IllegalStateException("No config value defined for manditory parameter " + paramName);
		}
		return paramValue;
	}
	
	public String[] getPlmnId() throws Exception {
		if ( plmnId == null ) {
			String plmnIdString = getManditoryParam("plmnId");
			plmnId = plmnIdString.split("-");
			if ( plmnId.length != 3 ) {
				throw new Exception("Invalid value for plmn:" + plmnIdString);
			}					
		}
			
		return plmnId;
	}
	
	public List<String[]> getPairs() throws Exception {
		List<String[]> result = new LinkedList<String[]>();
		
		boolean hasMoreSimPairs = true;
		int pairIndex = 1;
		while ( hasMoreSimPairs ) {
			String propName = "pair." + String.valueOf(pairIndex);
			String propValue = props.getProperty(propName);
			if ( propValue == null ) {
				hasMoreSimPairs = false;
			} else {
				String[] sims = propValue.split("@");
				if ( sims.length != 2 ) {
					throw new Exception("ERROR: Invalid value for " + propName + ": \"" + propValue + "\"");
				}
				result.add(sims);
				pairIndex++;
			}			
		}
		return result;
	}
}
