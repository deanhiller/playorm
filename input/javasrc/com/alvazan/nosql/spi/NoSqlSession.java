package com.alvazan.nosql.spi;

import java.util.List;

public interface NoSqlSession {

	public void persist(String colFamily, String rowKey, List<Column> columns);
	
	public void remove(String colFamily, String rowKey, List<Column> columns);
	
	public List<Row> find(String colFamily, List<String> key);
	
	public void flush();
}
