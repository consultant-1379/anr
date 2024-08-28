package com.ericsson.oss.anrx2.simulator.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface IStorable {
	public String getInsertStmt();
	public void bindVars(PreparedStatement ps) throws SQLException;
	
	public String getSQL( ChangeType change );
}
