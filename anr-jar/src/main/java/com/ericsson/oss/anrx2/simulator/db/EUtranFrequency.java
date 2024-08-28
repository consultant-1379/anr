package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class EUtranFrequency implements IStorable {
	public int enbId;
	public int arfcnValueEUtranDl;
	public String fdn;
	
	private final static Logger logger = Logger.getLogger(EUtranFrequency.class.getName()); 	
	
	public EUtranFrequency( int enbId, int arfcnValueEUtranDl, String fdn) {
		this.enbId = enbId;
		this.arfcnValueEUtranDl = arfcnValueEUtranDl;
		this.fdn = fdn;
	}
	
	// IStorable implemenation 
	private final static String insertStmt = "INSERT INTO enbEUtranFreq (enbId, arfcnValueEUtranDl, fdn) VALUES (?, ?, ?)";
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, enbId);
	    ps.setInt(2, arfcnValueEUtranDl);		    
	    ps.setString(3, fdn);
	}	

	public static List<EUtranFrequency> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT enbId, arfcnValueEUtranDl, fdn FROM enbEUtranFreq WHERE " + filter;
			logger.finer("getMatching: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<EUtranFrequency> result = new LinkedList<EUtranFrequency>();
			while ( rs.next() ) {
				EUtranFrequency match = new EUtranFrequency(rs.getInt("enbId"),rs.getInt("arfcnValueEUtranDl"),
						rs.getString("fdn"));
				result.add(match);
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
		throw new UnsupportedOperationException();		
	}
	
}
