package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class EUtranFreqRelation implements IStorable {
	public int enbId;
	public int cellId;
	public int arfcnValueEUtranDl;
	public int operRelations = 0;
	public int createdBy;	
	public String rdnId;
	
	private final static Logger logger = Logger.getLogger(ExternalEUtranCellFDD.class.getName()); 	
	
	
	public EUtranFreqRelation( int enbId, int cellId, int arfcnValueEUtranDl, int createdBy, String rdnId) {
		this.enbId = enbId;
		this.cellId = cellId;
		this.arfcnValueEUtranDl = arfcnValueEUtranDl;
		this.createdBy = createdBy;
		this.rdnId = rdnId;
	}

	// IStorable implemenation 
	private final static String insertStmt = "INSERT INTO eCellFddFreqRel (enbId, cellId, arfcnValueEUtranDl, operRelations, createdBy, rdnId) VALUES (?, ?, ?, ?, ?, ?)";
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, enbId);
	    ps.setInt(2, cellId);
	    ps.setInt(3, arfcnValueEUtranDl);
	    ps.setInt(4, operRelations);
	    ps.setInt(5, createdBy);		    
	    ps.setString(6, rdnId);
	}

	public static List<EUtranFreqRelation> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT enbId, cellId, arfcnValueEUtranDl, operRelations, createdBy, rdnId FROM eCellFddFreqRel WHERE " + filter;
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<EUtranFreqRelation> result = new LinkedList<EUtranFreqRelation>();
			while ( rs.next() ) {
				EUtranFreqRelation match = new EUtranFreqRelation(rs.getInt("enbId"),rs.getInt("cellId"),rs.getInt("arfcnValueEUtranDl"),						
						rs.getInt("createdBy"),rs.getString("rdnId"));
				match.operRelations = rs.getInt("operRelations");
				result.add(match);
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

	public String getSQL( ChangeType type ) {
		throw new UnsupportedOperationException();		
	}
	
}
