package com.ericsson.oss.anrx2.simulator.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;


public class CandidateRelatedCellData {
	private final static Logger logger = Logger.getLogger(CandidateRelatedCellData.class.getName());
	public static List<CandidateRelatedCellData> getCandidateRelated( int enbId, int cellId, int arfcnValueEUtranDl) throws Exception {
		Connection conn = Db.getInstance().getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sqlTemplate = "SELECT enbId,cellId,tac,physicalLayerCellIdGroup,physicalLayerSubCellId FROM candNeighborRel,eUCellFdd "
					+ "WHERE candNeighborRel.enbIdA = %d AND candNeighborRel.cellIdA = %d AND candNeighborRel.arfcnValueEUtranDl = %d AND "
					+ "candNeighborRel.enbIdB = eUCellFdd.enbId AND candNeighborRel.cellIdB = eUCellFdd.cellId";
			String sql = String.format(sqlTemplate, enbId, cellId, arfcnValueEUtranDl);
			logger.finer(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<CandidateRelatedCellData> result = new LinkedList<CandidateRelatedCellData>();
			while ( rs.next() ) {
				CandidateRelatedCellData match = new CandidateRelatedCellData(rs.getInt("enbId"),rs.getInt("cellId"),		    
						rs.getInt("physicalLayerCellIdGroup"),rs.getInt("physicalLayerSubCellId"),
						rs.getInt("tac"));						    								
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
	public final int cellId;
	public final int enbId;
	public final int physicalLayerCellIdGroup;
	public final int physicalLayerSubCellId;	
	public final int tac; 

	public CandidateRelatedCellData(int enbId, int cellId,
			int physicalLayerSubCellId, int physicalLayerCellIdGroup,
			int tac) {
		this.enbId = enbId;
		this.cellId = cellId;
		this.physicalLayerSubCellId = physicalLayerSubCellId;
		this.physicalLayerCellIdGroup = physicalLayerCellIdGroup;
		this.tac = tac;
	}					
}

