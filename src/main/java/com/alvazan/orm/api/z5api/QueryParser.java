package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.MetaLoader;
import com.alvazan.orm.layer5.query.ScannerForQuery;
import com.google.inject.ImplementedBy;

@ImplementedBy(ScannerForQuery.class)
public interface QueryParser {

	public SpiMetaQuery parseQueryForAdHoc(String query, MetaLoader mgr);
		
	/**
	 * For ORM layer that want to use TABLE as the alias instead of typing Entity name which allows
	 * you to refactor code without named queries breaking ;).
	 * 
	 * @param query the s-sql query
	 * @param targetTable name of the columnFamily
	 * @param errorMsg
	 * @return Meta Query for the provider
	 */
	public SpiMetaQuery parseQueryForOrm(String query, String targetTable, String errorMsg);
	
	public void close();
	
}
