package com.ericsson.oss.anrx2.simulator.engine.delete.reltype;

import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Config;

public class RelationDeleterFactory {
	private static RelationDeleterFactory instance;
	private final IRelationDeleterFactory implFactory;
	private final static Logger logger = Logger.getLogger(RelationDeleterFactory.class.getName()); 
	
	private RelationDeleterFactory() throws Exception {
        String createRelType = Config.getInstance().getManditoryParam("create.reltype");
        logger.fine("run: createRelType=" + createRelType);
        
        String factoryClassName = RelationDeleterFactory.class.getPackage().getName() + "." + createRelType + ".Factory";
        try {
        	implFactory = (IRelationDeleterFactory)Class.forName(factoryClassName).newInstance();
        } catch ( ClassNotFoundException cnfe ) {
            throw new Exception("Unknown value for create.reltype: " + createRelType);        	
        }		
	}
	
	public static synchronized RelationDeleterFactory getInstance() throws Exception {
		if ( instance == null ) {
			instance = new RelationDeleterFactory();
		}
		return instance;
	}
	
	public IRelationDeleter makeRelationDeleter() throws Exception {
		return implFactory.makeRelationDeleter();
	}
}
