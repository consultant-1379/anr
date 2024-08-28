package com.ericsson.oss.anrx2.simulator.db;

import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Config;

public class Db {
	private static Db inst;
	private List<Connection> connPool = new LinkedList<Connection>();
	
	private final static Logger logger = Logger.getLogger(Db.class.getName()); 
	private final static String dbConnStr = Config.getInstance().getManditoryParam("db.connection");
	
	public static synchronized Db getInstance() throws Exception {
		if ( inst == null ) {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			inst = new Db();
			inst.returnConnection(inst.getConnection(),true);
		}
			
		return inst;
	}
	
	private Db() {
	}
	
	
	public void loadChanges(Collection<DbChange> changes) throws Exception {
		Connection conn = getConnection();
		try {
			Statement stmt = conn.createStatement();
			for ( DbChange change : changes ) {
				String sql = change.data.getSQL(change.type);
				stmt.addBatch(sql);
				logger.fine(sql);
			}
			stmt.executeBatch();
			returnConnection(conn,true);
			conn = null;
		} finally {
			if ( conn != null ) {
				returnConnection(conn, false);
			}
		}
	}

	public int queryCount(String table, String filter) throws Exception {
		String sql = "SELECT COUNT(*) FROM " + table + " WHERE " + filter;
		Connection conn = getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			logger.finer("queryCount: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			int result = rs.getInt(1);
			logger.finer("queryCount: result=" + result);
			stmt.close();
			stmt = null;
			return result;
		} finally {
			if ( stmt == null ) {
				returnConnection(conn,true);
			} else {
				stmt.close();
				returnConnection(conn,false);
			}
		}
	}

	public void loadStorable(Collection<IStorable> data) throws Exception {
		if ( data.isEmpty() == true ) {
			return;
		}

		Connection conn = getConnection();
		try {
			int numInBatch = 0;		
			PreparedStatement ps = null;
			for ( IStorable one : data ) {
				if ( ps == null ) {
					ps = conn.prepareStatement(one.getInsertStmt());
				}
				one.bindVars(ps);

				ps.addBatch();
				numInBatch++;
				if ( numInBatch > 1000 ) {
					ps.executeBatch();
					numInBatch = 0;
				}			
			}
			if ( numInBatch > 0 )
				ps.executeBatch();

			ps.close();
			
			returnConnection(conn,true);
			conn = null;
		} finally {
			if ( conn != null ) {
				returnConnection(conn, false);
			}
		}
	}

	
	public Connection getConnection() throws Exception {
		Connection conn = null;
		synchronized (connPool) {
			if ( connPool.size() > 0 ) {
				conn = connPool.remove(0);
			}
		}
		if ( conn == null ) {
			conn = makeConnection();
		}
		return conn;
	}

	public void returnConnection(Connection aConn, boolean reuse) throws Exception {
		if ( reuse ) {
			synchronized (connPool) {
				connPool.add(aConn);
			}
		} else {
			aConn.close();
		}
	}
	
	public void shutdown() throws Exception {
		synchronized (connPool) {
			for ( Connection conn : connPool ) {
				conn.close();
			}
			connPool.clear();
		}		
	}
	private Connection makeConnection() throws Exception {
		return DriverManager.getConnection(dbConnStr, new Properties());
	}
	
}
