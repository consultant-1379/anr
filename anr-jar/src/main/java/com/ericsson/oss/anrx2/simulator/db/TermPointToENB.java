package com.ericsson.oss.anrx2.simulator.db;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class TermPointToENB implements IStorable {
	public int ownerEnbId;
	public int targetEnbId;
	public int createdBy;	

	private final static String insertStmt = "INSERT INTO termenb (ownerId, targetId, createdBy ) VALUES (?, ?, ?)";
	private final static Logger logger = Logger.getLogger(TermPointToENB.class.getName()); 
	
	public TermPointToENB( int ownerEnbId, int targetEnbId, int createdBy ) {
		this.ownerEnbId = ownerEnbId;
		this.targetEnbId = targetEnbId;
		this.createdBy = createdBy;
	}
		
	public String getInsertStmt() {
		return insertStmt;
	}

	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, ownerEnbId);
	    ps.setInt(2, targetEnbId);
	    ps.setInt(3, createdBy);
	}

	@Override
	public String getSQL(ChangeType type) {
		if ( type.equals(ChangeType.CREATE) ) {
			return String.format("INSERT INTO termenb (ownerId, targetId, createdBy) VALUES (%d, %d, %d)",
					ownerEnbId, targetEnbId, createdBy);			
		} else if ( type.equals(ChangeType.UPDATE) ) {
			return null;
		} else if ( type.equals(ChangeType.DELETE) ) {
			return String.format("DELETE FROM termenb WHERE ownerId = %d AND targetId = %d",ownerEnbId, targetEnbId);			
		} else {
			return null;
		}		
	}
	
	public static List<TermPointToENB> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT ownerId, targetId, createdBy FROM termenb WHERE " + filter;
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<TermPointToENB> result = new LinkedList<TermPointToENB>();
			while ( rs.next() ) {
				TermPointToENB match = new TermPointToENB(rs.getInt("ownerId"),rs.getInt("targetId"),
						rs.getInt("createdBy"));
				result.add(match);
				if ( logger.isLoggable(Level.FINER) ) logger.finer("getMatching matched: " + match);
			}
			stmt.close();
			stmt = null;
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

}
