package com.alvazan.orm.api.spi9.db;

import java.util.List;
import java.util.Map;



public interface NoSqlRawSession {
	
	/**
	 * Don't you dare find one key at a time if you implement this.  
	 * Find them in parallel as all nosql systems support that.
	 * 
	 * @param colFamily
	 * @param key
	 * @return
	 */
	public Iterable<KeyValue<Row>> find2(String colFamily, Iterable<byte[]> rowKeys);
	
	/**
	 * Action is subclassed by Remove and Persist and will be executed
	 * in the order we are given here
	 * @param actions
	 */
	public void sendChanges(List<Action> actions, Object session);

	public void clearDatabase();

	public void start(Map<String, Object> properties);
	
	public void close();

	/**
	 * 
	 * @param scan
	 * @param from If null, it means open ended otherwise we scan from this key inclusive or exclusively depending on the field inside the Key
	 * @param to If null, it means open ended otherwise we scan to this key inclusive or exclusively depending on the field inside the Key
	 * @param batchSize TODO
	 * @return
	 */
	public Iterable<Column> columnRangeScan(ScanInfo scan, Key from, Key to, int batchSize);

}
