package com.alvazan.orm.api.z8spi;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;



public interface NoSqlRawSession {
	
	/**
	 * Action is subclassed by Remove and Persist and will be executed
	 * in the order we are given here
	 * @param actions
	 */
	public void sendChanges(List<Action> actions, MetaLookup session);

	public void clearDatabase();

	public void start(Map<String, Object> properties);
	
	public void close();

	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize, BatchListener l, MetaLookup mgr);
	
	/**
	 * 
	 * @param scan
	 * @param from If null, it means open ended otherwise we scan from this key inclusive or exclusively depending on the field inside the Key
	 * @param to If null, it means open ended otherwise we scan to this key inclusive or exclusively depending on the field inside the Key
	 * @param batchSize 
	 * @return
	 */
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from, Key to, Integer batchSize, BatchListener l, MetaLookup mgr);

	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values, BatchListener list, MetaLookup mgr);

	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily, DirectCursor<byte[]> rowKeys, Cache cache, int batchSize, BatchListener list, MetaLookup mgr);

	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily);

}
