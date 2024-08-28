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

import com.ericsson.oss.anrx2.simulator.engine.CellIdentity;

public class CandNeighborRel implements ICellRelation {
	public int enbIdA;
	public int cellIdA;
	public int enbIdB;
	public int cellIdB;
	public int arfcnValueEUtranDl;
	
	public int dist;
	
	public Timestamp timeOfCreation;
	
	private final static String insertStmt = "INSERT INTO candNeighborRel (enbIdA,cellIdA,enbIdB,cellIdB,arfcnValueEUtranDl,dist,timeOfCreation) VALUES (?,?,?,?,?,?,?)";
	
	private final static Logger logger = Logger.getLogger(CandNeighborRel.class.getName()); 
	
	public CandNeighborRel( int enbIdA, int cellIdA, int enbIdB, int cellIdB, int arfcnValueEUtranDl, int dist, Date timeOfCreation ) {
		this.enbIdA = enbIdA;
		this.cellIdA = cellIdA;
		this.enbIdB = enbIdB;
		this.cellIdB = cellIdB;
		this.arfcnValueEUtranDl = arfcnValueEUtranDl;
		this.dist = dist;
		this.timeOfCreation = new Timestamp(timeOfCreation.getTime());		
	}
	
	public String getTrafIdA() {
		return String.valueOf(enbIdA) + "-" + String.valueOf(cellIdA);
	}
	
	public String getTrafIdB() {
		return String.valueOf(enbIdB) + "-" + String.valueOf(cellIdB);
	}
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
		ps.setInt(1, enbIdA);
		ps.setInt(2, cellIdA);
		ps.setInt(3, enbIdB);
		ps.setInt(4, cellIdB);
		ps.setInt(5, arfcnValueEUtranDl);	
		ps.setInt(6, dist);
	    ps.setTimestamp(7, timeOfCreation);		
	}
	
	public static List<CandNeighborRel> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT enbIdA,cellIdA,enbIdB,cellIdB,arfcnValueEUtranDl,dist,timeOfCreation FROM candNeighborRel WHERE " + filter;
			logger.finer("getMatching: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<CandNeighborRel> result = new LinkedList<CandNeighborRel>();
			while ( rs.next() ) {
				CandNeighborRel match = new CandNeighborRel(rs.getInt("enbIdA"),rs.getInt("cellIdA"),
						rs.getInt("enbIdB"),rs.getInt("cellIdB"),rs.getInt("arfcnValueEUtranDl"),
						rs.getInt("dist"),
						rs.getTimestamp("timeOfCreation"));		
				result.add(match);
			}
			stmt.close();
			logger.finer("getMatching: result.size()=" + result.size());			
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
		if ( type.equals(ChangeType.CREATE) ) {
			return String.format("INSERT INTO candNeighborRel (enbIdA,cellIdA,enbIdB,cellIdB,arfcnValueEUtranDl,dist,timeOfCreation) VALUES (%d,%d,%d,%d,%d,%d,'%s')",
					enbIdA,cellIdA,enbIdB,cellIdB,arfcnValueEUtranDl,dist,TimeOfCreation.getInstance().format(timeOfCreation));			
		} else if ( type.equals(ChangeType.UPDATE) ) {
			return String.format("UPDATE candNeighborRel SET dist = %d WHERE enbIdA = %d AND cellIdA = %d AND enbIdB = %d AND cellIdB = %d ",dist,enbIdA,cellIdA,enbIdB,cellIdB);
		} else if ( type.equals(ChangeType.DELETE) ) {
			return String.format("DELETE FROM candNeighborRel WHERE enbIdA = %d AND cellIdA = %d AND enbIdB = %d AND cellIdB = %d ",enbIdA,cellIdA,enbIdB,cellIdB);			
		} else {
			return null;
		}
	}
	
	public String toString() {
		return enbIdA + "-" + cellIdA + "->" + enbIdB + "-" + cellIdB;
	}

	@Override
	public CellIdentity getCellA() {
		return new CellIdentity(enbIdA,cellIdA);
	}

	@Override
	public CellIdentity getCellB() {
		return new CellIdentity(enbIdB,cellIdB);		
	}

	@Override
	public int getDistance() {
		return dist;
	}

	@Override
	public void setDistance(int dist) {
		this.dist = dist;
	}	
}
