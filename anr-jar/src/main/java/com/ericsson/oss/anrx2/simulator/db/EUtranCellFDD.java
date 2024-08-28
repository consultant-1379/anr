package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.celllocation.CellLocation;

public class EUtranCellFDD implements IStorable {
	public int enbId;
	public int cellId;
	public int tac;
	
	public int physicalLayerCellIdGroup;
	public int physicalLayerSubCellId;

	public int ulChannelBandwidth;
	public int dlChannelBandwidth;	
	public int earfcnul;
	public int earfcndl;
	
	public String fdn;
	
	public int operRelations = 0;
	
	public CellLocation location;
	
	private final static String insertStmt = "INSERT INTO eUCellFdd (enbId,cellId,tac,physicalLayerCellIdGroup,physicalLayerSubCellId,ulChannelBandwidth,dlChannelBandwidth,earfcnul,earfcndl,latitude,longitude,altitude,operRelations,fdn) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private final static Logger logger = Logger.getLogger(EUtranCellFDD.class.getName()); 
	
	public EUtranCellFDD( int enbId, int cellId, int tac, int physicalLayerCellIdGroup, int physicalLayerSubCellId, 
			int ulChannelBandwidth, int dlChannelBandwidth, int earfcnul, int earfcndl, 
			CellLocation location, String fdn ) {
		this.enbId = enbId;
		this.cellId = cellId;
		this.tac = tac;
		this.physicalLayerCellIdGroup = physicalLayerCellIdGroup;
		this.physicalLayerSubCellId = physicalLayerSubCellId;
		this.ulChannelBandwidth = ulChannelBandwidth;
		this.dlChannelBandwidth = dlChannelBandwidth;
		this.earfcnul = earfcnul;
		this.earfcndl = earfcndl;
		this.location = location;
		this.fdn = fdn;	
	}
	
	public String getInsertStmt() {
		return insertStmt;
	}
	
	public void bindVars(PreparedStatement ps) throws SQLException {
	    ps.setInt(1, enbId);
	    ps.setInt(2, cellId);
	    ps.setInt(3, tac);		    
	    ps.setInt(4, physicalLayerCellIdGroup);
	    ps.setInt(5, physicalLayerSubCellId);		    
	    ps.setInt(6, ulChannelBandwidth);		    
	    ps.setInt(7, dlChannelBandwidth);	
	    ps.setInt(8, earfcnul);
	    ps.setInt(9, earfcndl);
	    ps.setInt(10, location.latitude);
	    ps.setInt(11, location.longitude);
	    ps.setInt(12, location.altitude);
	    ps.setInt(13, operRelations);	
	    ps.setString(14, fdn);		    
	}
	
	public static List<EUtranCellFDD> getMatching(String filter) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "SELECT enbId,cellId,tac,physicalLayerCellIdGroup,physicalLayerSubCellId,ulChannelBandwidth,dlChannelBandwidth,earfcnul,earfcndl,latitude,longitude,altitude,operRelations,fdn FROM eUCellFdd WHERE " + filter;
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<EUtranCellFDD> result = new LinkedList<EUtranCellFDD>();
			while ( rs.next() ) {
				EUtranCellFDD match = new EUtranCellFDD(rs.getInt("enbId"),rs.getInt("cellId"),rs.getInt("tac"),		    
					    								rs.getInt("physicalLayerCellIdGroup"),rs.getInt("physicalLayerSubCellId"), 
					    								rs.getInt("ulChannelBandwidth"), rs.getInt("dlChannelBandwidth"),
					    								rs.getInt("earfcnul"), rs.getInt("earfcndl"),
					    								new CellLocation(rs.getInt("latitude"), rs.getInt("longitude"), rs.getInt("altitude")),
					    								rs.getString("fdn"));
				match.operRelations = rs.getInt("operRelations");
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
		if ( type.equals(ChangeType.UPDATE) ) {
			return String.format("UPDATE eUCellFdd SET latitude = %d, longitude = %d, altitude = %d WHERE enbId = %d AND cellId = %d",
					location.latitude, location.longitude, location.altitude, enbId, cellId);			
		} else {
			throw new UnsupportedOperationException();
		}
	}		
}
