package com.ericsson.oss.anrx2.simulator.engine;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface IEngineControl extends Remote {
	public String[] execute(String command, Map<String,String> parameters) throws RemoteException;
}
