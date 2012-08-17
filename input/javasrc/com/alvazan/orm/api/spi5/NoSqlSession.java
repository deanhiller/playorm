package com.alvazan.orm.api.spi5;

import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;
import com.alvazan.orm.layer5.nosql.cache.NoSqlWriteCacheImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlWriteCacheImpl.class)
public interface NoSqlSession {

	/**
	 * Retrieves the rawest interface that all the providers implement(in-memory, cassandra, hadoop, etc) BUT
	 * be warned, when you call persist, it is executed IMMEDIATELY, there is no flush method on that
	 * interface.  In fact, the flush methods of all higher level interfaces call persist ONCE on the raw
	 * session and providers are supposed to do ONE send to avoid network latency if you are sending 100
	 * actions to the nosql database.
	 * @return
	 */
	public NoSqlRawSession getRawSession();
	
	public void persistIndex(String colFamily, String indexColFamily, byte[] rowKey, IndexColumn column);
	public void removeFromIndex(String colFamily, String indexColFamily, byte[] rowKeyBytes, IndexColumn c);
	
	public void put(String colFamily, byte[] rowKey, List<Column> columns);

	/**
	 * Remove entire row.
	 * 
	 * @param colFamily
	 * @param rowKey
	 */
	public void remove(String colFamily, byte[] rowKey);
	
	/**
	 * Remove specific columns from a row, mainly 
	 * 
	 * @param colFamily
	 * @param rowKey
	 * @param columns
	 */
	public void remove(String colFamily, byte[] rowKey, Collection<byte[]> columnNames);
	
	public List<Row> find(String colFamily, List<byte[]> rowKeys);
	
	public Iterable<KeyValue<Row>> find(String colFamily, Iterable<byte[]> rowKeys);
	
	public Row find(String colFamily, byte[] rowKey);
	
	public void flush();

	public void clearDb();

	/**
	 * Returns a special Iterable loads itself first with "batchSize" records and then after you
	 * iterator over those and request "batchSize+1" record, it hits the database again such that
	 * your memory does not explode if you have a huge amount of records.
	 * @param from
	 * @param to
	 * @param batchSize TODO
	 * @param colFamily
	 * @param rowKey
	 * @return
	 */
	public Iterable<Column> columnRangeScan(ScanInfo scanInfo, Key from, Key to, int batchSize);

	public void setOrmSessionForMeta(Object entityMgr);

}
