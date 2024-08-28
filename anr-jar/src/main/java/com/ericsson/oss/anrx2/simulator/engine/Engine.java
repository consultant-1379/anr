package com.ericsson.oss.anrx2.simulator.engine;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.engine.create.CreatorTimer;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreator;
import com.ericsson.oss.anrx2.simulator.engine.create.random.IRandomRelationCreatorFactory;
import com.ericsson.oss.anrx2.simulator.engine.delete.DeleteFactory;
import com.ericsson.oss.anrx2.simulator.netsim.SimConnectionFactory;

public class Engine {
	private final static Logger logger = Logger.getLogger(Engine.class.getName()); 
	private CreatorTimer creator;
	
	private Thread deleterThread;
	private Thread creatorThread;
	
	public static void main(String args[]) {
		try {
			new Engine().run(args);
			System.exit(0);
		} catch ( Throwable t ) {
			t.printStackTrace();
			System.exit(1);
		}		
	}

    private void run(String[] args) throws Exception {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
         
        // Setup RMI ctrl interface
        EngineControl engCtrl = new EngineControl(this);
        IEngineControl stub = (IEngineControl) UnicastRemoteObject.exportObject(engCtrl, 0);
        int rmiRegPort = Integer.parseInt(Config.getInstance().getManditoryParam("rmiRegPort"));
        Registry registry = LocateRegistry.createRegistry(rmiRegPort);
        registry.rebind("EngineControl", stub);        
        
        // Setup Relation Creation
        String createMode = Config.getInstance().getManditoryParam("create.mode");
        logger.fine("run: createMode=" + createMode);
        
        String factoryClassName = IRandomRelationCreatorFactory.class.getPackage().getName() + "." + createMode + ".Factory";
        IRandomRelationCreatorFactory relFactory = null;
        try {
        	relFactory = (IRandomRelationCreatorFactory)Class.forName(factoryClassName).newInstance();
        } catch ( ClassNotFoundException cnfe ) {
            throw new Exception("Unknown value for create.mode: " + createMode);        	
        }
        
    	AtomicBoolean exitFlag = ThreadManager.getInstance().getExitFlag();
        
        IRandomRelationCreator relCreator = relFactory.create();
        creator = new CreatorTimer(exitFlag,relCreator);
        creatorThread = new Thread(creator);
        ThreadManager.getInstance().registerThread(creatorThread);
        
        // Setup Deletion
        deleterThread = new Thread(DeleteFactory.getDeleter(exitFlag));
        ThreadManager.getInstance().registerThread(deleterThread);

        creatorThread.start();
        deleterThread.start();
        
        // Wait till we're shutdown
        logger.info("run waiting");
        synchronized(this) {
            this.wait();
        }
                
        logger.info("run shutting down");
        ThreadManager.getInstance().shutdown();
        
        SimConnectionFactory.getInstance().shutdown();
        Db.getInstance().shutdown();
                
        logger.info("run returning");
    }


    public void shutdown() {
        logger.info("shutdown called");
        synchronized(this) {
            this.notify();
        }
    }        
}
