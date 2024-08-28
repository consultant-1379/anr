package com.ericsson.oss.anrx2.simulator.engine;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EngineControl implements IEngineControl {
	private final static Logger logger = Logger.getLogger(EngineControl.class.getName()); 

	Engine engine;
	EngineControl(Engine engine) {
		this.engine = engine;
	}
	
	@Override
	public String[] execute(String command, Map<String, String> parameters)
			throws RemoteException {
		logger.fine("execute: command=" + command);
		for ( String parameterName : parameters.keySet() ) {
			logger.fine("execute: parameter " + parameterName + "=" + parameters.get(parameterName));			
		}
		
		String commandClassName = EngineControl.class.getPackage().getName() + ".command." +
				(Character.toUpperCase(command.charAt(0)) + command.substring(1));
		logger.fine("execute: commandClassName=" + commandClassName);
		ICommandHandler cmdHandler = null;
		try {
			cmdHandler = (ICommandHandler)Class.forName(commandClassName).newInstance();
		} catch ( Exception e ) {
			throw new RemoteException("Unknown command: " + command);
		}
		
		try {
			return cmdHandler.execute(parameters, engine);
		} catch ( Exception e ) {
			logger.log(Level.WARNING, "execute", e);
			String msg = e.getMessage();
			throw new RemoteException("Command failed: " + msg);			
		}
	}
}
