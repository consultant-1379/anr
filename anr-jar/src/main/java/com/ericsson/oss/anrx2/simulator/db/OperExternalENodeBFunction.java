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

public class OperExternalENodeBFunction implements IStorable {
	private int targetId;
	private int ownerId;
	
	public OperExternalENodeBFunction(int ownerId, int targetId) {
		this.ownerId = ownerId;
		this.targetId = targetId;
	}
	
	public String getInsertStmt() {
		return insertStmt;
	}

	public void bindVars(PreparedStatement ps) throws SQLException {
		ps.setInt(1, ownerId);
		ps.setInt(2, targetId);
	}

	public String getSQL(ChangeType change) {
		throw new UnsupportedOperationException();
	}
	
	public static List<OperExternalENodeBFunction> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT ownerId, targetId FROM oper_eenb WHERE " + filter;
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<OperExternalENodeBFunction> result = new LinkedList<OperExternalENodeBFunction>();
			while ( rs.next() ) {
				OperExternalENodeBFunction match = new OperExternalENodeBFunction(rs.getInt("ownerId"),rs.getInt("targetId"));
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
	
	public String toString() {
		return String.valueOf(ownerId) + "-" + String.valueOf(targetId);
	}
	
	private final static String insertStmt = "INSERT INTO oper_eenb (ownerId, targetId) VALUES (?, ?)";
	private final static Logger logger = Logger.getLogger(OperExternalENodeBFunction.class.getName()); 	
}
