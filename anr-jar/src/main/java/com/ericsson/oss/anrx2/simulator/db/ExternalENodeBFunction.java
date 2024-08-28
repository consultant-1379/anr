package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExternalENodeBFunction implements IStorable {
	public int ownerEnbId;
	public int targetEnbId;
	public int createdBy;
	public Timestamp lastUpdated;
	
	public ExternalENodeBFunction( int ownerEnbId, int targetEnbId, int createdBy, Date lastUpdated) {
		this.ownerEnbId = ownerEnbId;
		this.targetEnbId = targetEnbId;
		this.createdBy = createdBy;
		if ( lastUpdated != null ) {
			this.lastUpdated = new Timestamp(lastUpdated.getTime());
		}
	}
	
	// IStorable implemenation 
	private final static String insertStmt = "INSERT INTO eenb (ownerId, targetId, createdBy, lastUpdated) VALUES (?, ?, ?, ?)";
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, ownerEnbId);
	    ps.setInt(2, targetEnbId);
	    ps.setInt(3, createdBy);
	    ps.setTimestamp(4, lastUpdated);	    
	}	
	
	public static int getMatchingCount(String filter) throws Exception {
		return Db.getInstance().queryCount("eenb",filter);
	}
		
	public static List<ExternalENodeBFunction> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT ownerId, targetId, createdBy, lastUpdated FROM eenb WHERE " + filter;
			logger.finer("getMatching: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<ExternalENodeBFunction> result = new LinkedList<ExternalENodeBFunction>();
			while ( rs.next() ) {
				ExternalENodeBFunction match = new ExternalENodeBFunction(rs.getInt("ownerId"),rs.getInt("targetId"),						
						rs.getInt("createdBy"), rs.getTimestamp("lastUpdated"));
				result.add(match);
				if ( logger.isLoggable(Level.FINER) ) logger.finer("getMatching matched: " + match);
			}
			stmt.close();
			stmt = null;
			logger.finer("getMatching: result.size()=" + result.size());						
			return result;
		} finally {
			if ( stmt == null ) {
				Db.getInstance().returnConnection(conn,true);
			} else {
				stmt.close();
				Db.getInstance().returnConnection(conn,false);
			}
		}
	}
	
	public String getSQL( ChangeType type ) {
		if ( type.equals(ChangeType.CREATE) ) {
			return String.format("INSERT INTO eenb (ownerId, targetId, createdBy, lastUpdated) VALUES (%d, %d, %d, '%s')",
					ownerEnbId, targetEnbId, createdBy, TimeOfCreation.getInstance().format(lastUpdated));			
		} else if ( type.equals(ChangeType.UPDATE) ) {
			return String.format("UPDATE eenb SET lastUpdated = '%s' WHERE ownerId = %d AND targetId = %d",
					TimeOfCreation.getInstance().format(lastUpdated), ownerEnbId, targetEnbId);			
		} else if ( type.equals(ChangeType.DELETE) ) {
			return String.format("DELETE FROM eenb WHERE ownerId = %d AND targetId = %d",ownerEnbId, targetEnbId);			
		} else {
			return null;
		}
	}
	
	public String toString() {
		return String.valueOf(ownerEnbId) + "-" + String.valueOf(targetEnbId) + " ";
	}

	private final static Logger logger = Logger.getLogger(ExternalENodeBFunction.class.getName()); 
}
