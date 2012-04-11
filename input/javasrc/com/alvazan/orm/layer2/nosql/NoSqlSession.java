package com.alvazan.orm.layer2.nosql;

import java.util.List;

import com.alvazan.orm.layer2.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer3.spi.Column;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlWriteCacheImpl.class)
public interface NoSqlSession {

	public void persist(String colFamily, String rowKey, List<Column> columns);

	/**
	 * Remove entire row.
	 * 
	 * @param colFamily
	 * @param rowKey
	 */
	public void remove(String colFamily, String rowKey);
	
	/**
	 * Remove specific columns from a row
	 * 
	 * @param colFamily
	 * @param rowKey
	 * @param columns
	 */
	public void remove(String colFamily, String rowKey, List<String> columnNames);
	
	public List<Row> find(String colFamily, List<String> rowKeys);
	
	public void flush();

}
