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
import java.util.logging.Logger;

public class ExternalEUtranCellFDD implements IStorable {
	public final int ownerEnbId;
	public final int targetEnbId;
	public final int localCellId;

	public final int createdBy;	
	public Timestamp lastUpdated;
	
	private final static Logger logger = Logger.getLogger(ExternalEUtranCellFDD.class.getName()); 	
	
	public ExternalEUtranCellFDD( int ownerEnbId, int targetEnbId, int localCellId, int createdBy, Date lastUpdated) {
		this.ownerEnbId = ownerEnbId;
		this.targetEnbId = targetEnbId;
		this.localCellId = localCellId;
		this.createdBy = createdBy;
		if ( lastUpdated != null ) {
			this.lastUpdated = new Timestamp(lastUpdated.getTime());
		}		
	}
	
	// IStorable implemenation 
	private final static String insertStmt = "INSERT INTO eeCellFdd (ownerId, targetId, localCellId, createdBy, lastUpdated) VALUES (?, ?, ?, ?, ?)";
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, ownerEnbId);
	    ps.setInt(2, targetEnbId);
	    ps.setInt(3, localCellId);	
	    ps.setInt(4, createdBy);
	    ps.setTimestamp(5, lastUpdated);
	}	

	public static int getMatchingCount(String filter) throws Exception {
		return Db.getInstance().queryCount("eeCellFdd",filter);
	}
	
	public static List<ExternalEUtranCellFDD> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT ownerId, targetId, localCellId, refCount, createdBy, lastUpdated FROM eeCellFdd WHERE " + filter;
			logger.finer("getMatching: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<ExternalEUtranCellFDD> result = new LinkedList<ExternalEUtranCellFDD>();
			while ( rs.next() ) {
				ExternalEUtranCellFDD match = new ExternalEUtranCellFDD(rs.getInt("ownerId"),rs.getInt("targetId"),rs.getInt("localCellId"),
						rs.getInt("createdBy"),rs.getTimestamp("lastUpdated"));
				result.add(match);
				logger.finer("matched " + match.toString());
			}
			stmt.close();
			stmt = null;
			logger.finer("getMatching result.size=" + result.size());
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
			return String.format("INSERT INTO eeCellFdd (ownerId, targetId, localCellId, createdBy, lastUpdated) VALUES (%d, %d, %d, %d, '%s')",
					ownerEnbId, targetEnbId, localCellId, createdBy, TimeOfCreation.getInstance().format(lastUpdated));			
		} else if ( type.equals(ChangeType.UPDATE) ) {
			return String.format("UPDATE eeCellFdd SET lastUpdated = '%s' WHERE ownerId = %d AND targetId = %d AND localCellId = %d",
					TimeOfCreation.getInstance().format(lastUpdated), ownerEnbId, targetEnbId, localCellId);			
		} else if ( type.equals(ChangeType.DELETE) ) {
			return String.format("DELETE FROM eeCellFdd WHERE ownerId = %d AND targetId = %d  AND localCellId = %d",ownerEnbId, targetEnbId, localCellId);			
		} else {
			return null;
		}
	}

	public String toString() {
		return String.format("%d-%d-%d", ownerEnbId, targetEnbId, localCellId);
	}
}
