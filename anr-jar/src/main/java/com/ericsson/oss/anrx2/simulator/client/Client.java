package com.ericsson.oss.anrx2.simulator.client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

import com.ericsson.oss.anrx2.simulator.engine.IEngineControl;

public class Client {

	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		String cmd = args[1];
		Map<String,String> parameters = new HashMap<>();
		for ( int i = 2; i < args.length; i += 2 ) {
			parameters.put(args[i], args[i+1]);
		}
		
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }		
		try {
            String name = "EngineControl";
            Registry registry = LocateRegistry.getRegistry(port);
            IEngineControl ctrl = (IEngineControl) registry.lookup(name);
            
            String[] result = ctrl.execute(cmd, parameters);
            if ( result != null ) {
            	for ( String line : result ) {
            		System.out.println(line);
            	}
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }		
	}

}
