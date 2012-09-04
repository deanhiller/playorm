package com.alvazan.orm.api.z8spi;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;



public interface NoSqlRawSession {
	
	/**
	 * Don't you dare find one key at a time if you implement this.  
	 * Find them in parallel as all nosql systems support that.
	 * 
	 * @param colFamily
	 * @param key
	 * @return
	 */
	public Iterable<KeyValue<Row>> find(String colFamily, Iterable<byte[]> rowKeys);
	
	/**
	 * Action is subclassed by Remove and Persist and will be executed
	 * in the order we are given here
	 * @param actions
	 */
	public void sendChanges(List<Action> actions, MetaLookup session);

	public void clearDatabase();

	public void start(Map<String, Object> properties);
	
	public void close();

	public AbstractCursor<Column> columnSlice(String colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize, BatchListener l);
	
	/**
	 * 
	 * @param scan
	 * @param from If null, it means open ended otherwise we scan from this key inclusive or exclusively depending on the field inside the Key
	 * @param to If null, it means open ended otherwise we scan to this key inclusive or exclusively depending on the field inside the Key
	 * @param batchSize 
	 * @return
	 */
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from, Key to, Integer batchSize, BatchListener l);

}
