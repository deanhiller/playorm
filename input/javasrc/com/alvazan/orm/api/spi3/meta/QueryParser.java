package com.alvazan.orm.api.spi3.meta;

import com.alvazan.orm.layer5.nosql.cache.QueryParserImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(QueryParserImpl.class)
public interface QueryParser {

	public MetaAndIndexTuple parseQueryForAdHoc(String query, Object mgr);
		
	/**
	 * For ORM layer that want to use TABLE as the alias instead of typing Entity name which allows
	 * you to refactor code without named queries breaking ;).
	 * 
	 * @param query
	 * @param targetTable
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public MetaQuery parseQueryForOrm(String query, String targetTable);
	
	public void close();
	
}
