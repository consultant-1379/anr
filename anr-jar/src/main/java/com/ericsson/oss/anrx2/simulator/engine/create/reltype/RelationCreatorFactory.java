package com.ericsson.oss.anrx2.simulator.engine.create.reltype;

import java.util.List;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.ICellRelation;
import com.ericsson.oss.anrx2.simulator.engine.Config;

public class RelationCreatorFactory {
	private static RelationCreatorFactory instance;
	private IRelationCreatorFactory relFactory;
	
	private final static Logger logger = Logger.getLogger(RelationCreatorFactory.class.getName()); 

	public static synchronized  RelationCreatorFactory getInstance() throws Exception {
		if ( instance == null ) {
			instance = new RelationCreatorFactory();
		}
		
		return instance;
	}
	
	public IRelationCreator makeRelationCreator() throws Exception {
		return relFactory.makeRelationCreator();
	}
	
	public List<ICellRelation> getRelations(String filter) throws Exception {
		return relFactory.getRelations(filter);
	}
	
	private RelationCreatorFactory() throws Exception {
        String createRelType = Config.getInstance().getManditoryParam("create.reltype");
        logger.fine("run: createRelType=" + createRelType);
        
        String factoryClassName = RelationCreatorFactory.class.getPackage().getName() + "." + createRelType + ".Factory";
        try {
        	relFactory = (IRelationCreatorFactory)Class.forName(factoryClassName).newInstance();
        } catch ( ClassNotFoundException cnfe ) {
            throw new Exception("Unknown value for create.reltype: " + createRelType);        	
        }
        
	}
}
