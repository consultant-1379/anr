package com.ericsson.oss.anrx2.simulator.engine.command;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.db.Db;
import com.ericsson.oss.anrx2.simulator.engine.Engine;
import com.ericsson.oss.anrx2.simulator.engine.ICommandHandler;

public class Query implements ICommandHandler {
	private final static Logger logger = Logger.getLogger(Query.class.getName()); 

	@Override
	public String[] execute(Map<String, String> parameters, Engine engine) {
		String sql = parameters.get("sql");
		logger.info("query sql:" + sql);
		
		String result[] = null;

		Connection conn = null;
		try {
			conn = Db.getInstance().getConnection();
			Statement stmt = conn.createStatement();

			if ( sql.startsWith("SELECT") ) {
				ResultSet rs = stmt.executeQuery(sql);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numCols = rsmd.getColumnCount();
				List<String> results = new LinkedList<String>();
				while ( rs.next() ) {
					StringBuffer resultLine = new StringBuffer();
					for ( int colIndex = 1; colIndex <= numCols; colIndex ++ ) {
						if ( colIndex > 1)
							resultLine.append(";");
						resultLine.append(rs.getObject(colIndex));
					}
					results.add(resultLine.toString());
				}
				result = results.toArray(new String[results.size()]);
				logger.info("query returning " + results.size() + " results");				
			} else {
				stmt.executeUpdate(sql);
				result = new String[0];
			}
			stmt.close();
			stmt = null;
			
			
			return result;
		} catch ( Throwable t ) {
			t.printStackTrace();
			return new String[] { t.getMessage() };
		}finally {
			if ( conn != null ) {
				try {
					Db.getInstance().returnConnection(conn, false);
				} catch ( Exception ignored ) {}
			}
		}
	}

}
