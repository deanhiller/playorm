package com.alvazan.orm.api.spi.db;

import java.util.List;


public interface NoSqlRawSession {
	
	/**
	 * Don't you dare find one key at a time if you implement this.  
	 * Find them in parallel as all nosql systems support that.
	 * 
	 * @param colFamily
	 * @param key
	 * @return
	 */
	public List<Row> find(String colFamily, List<byte[]> key);
	
	/**
	 * Action is subclassed by Remove and Persist and will be executed
	 * in the order we are given here
	 * @param actions
	 */
	public void sendChanges(List<Action> actions);

	public void clearDatabaseIfInMemoryType();
}
