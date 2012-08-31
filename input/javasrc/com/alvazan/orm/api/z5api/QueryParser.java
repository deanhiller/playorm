package com.alvazan.orm.api.z5api;

import com.alvazan.orm.layer5.query.ScannerForQuery;
import com.google.inject.ImplementedBy;

@ImplementedBy(ScannerForQuery.class)
public interface QueryParser {

	public SpiMetaQuery parseQueryForAdHoc(String query, Object mgr);
		
	/**
	 * For ORM layer that want to use TABLE as the alias instead of typing Entity name which allows
	 * you to refactor code without named queries breaking ;).
	 * 
	 * @param query
	 * @param targetTable
	 * @return
	 */
	public SpiMetaQuery parseQueryForOrm(String query, String targetTable, String errorMsg);
	
	public void close();
	
}
