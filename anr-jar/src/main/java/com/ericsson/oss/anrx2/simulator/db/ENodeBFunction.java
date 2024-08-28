package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class ENodeBFunction implements IStorable {	
	public ENodeBFunction(String fdn, int eNBId, String ipAddress, String sim,String neMIMVersion) {
		this.fdn = fdn;
		this.eNBId = eNBId;
		this.ipAddress = ipAddress;	
		this.sim = sim;
		this.neMIMVersion=neMIMVersion;
	}

	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, eNBId);
	    ps.setString(2, fdn);
	    ps.setString(3, ipAddress);
	    ps.setString(4, sim);
	    ps.setString(5, neMIMVersion);
	    ps.setInt(6, operExternEnbFunc);
	}
	
	public static List<ENodeBFunction> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT eNBId, fdn, ip, sim, neMIMVersion, operExternEnbFunc FROM enb WHERE " + filter;
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<ENodeBFunction> result = new LinkedList<ENodeBFunction>();
			while ( rs.next() ) {
				ENodeBFunction match = new ENodeBFunction(rs.getString("fdn"),rs.getInt("eNBId"),rs.getString("ip"),rs.getString("sim"),rs.getString("neMIMVersion"));
				match.operExternEnbFunc = rs.getInt("operExternEnbFunc");
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
	
	public String fdn;
	public String ipAddress;
	public int eNBId;
	public int operExternEnbFunc = 0;
	public String sim;
	public String neMIMVersion;
	
	private final static String insertStmt = "INSERT INTO enb (eNBId, fdn, ip, sim, neMIMVersion, operExternEnbFunc) VALUES (?, ?, ?, ?, ?, ?)";
	private final static Logger logger = Logger.getLogger(ENodeBFunction.class.getName()); 
}
