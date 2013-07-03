package com.alvazan.orm.api.z3api;

import java.math.BigInteger;
import java.util.List;

import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.layer3.typed.NoSqlTypedSessionImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlTypedSessionImpl.class)
public interface NoSqlTypedSession {
	
	public TypedRow createTypedRow(String columnFamily);
	
	/**
	 * Retrieves the rawest interface that all the providers implement(in-memory, cassandra, hadoop, etc) BUT
	 * be warned, when you call persist, it is executed IMMEDIATELY, there is no flush method on that
	 * interface.  In fact, the flush methods of all higher level interfaces call persist ONCE on the raw
	 * session and providers are supposed to do ONE send to avoid network latency if you are sending 100
	 * actions to the nosql database.
	 * @return The raw session that all the providers implement
	 */
	public NoSqlSession getRawSession();

	/**
	 * This API takes BigDecimal, BigInteger, or String as the types and no others for the rowKey
	 * @param colFamily		ColumnFamily where the row will be saved.
	 * @param row	 	    <code>TypedRow</code> having a rowkey which is BigDecimal, BigInteger, or String
	 */
	public void put(String colFamily, TypedRow row);
	
	/**
	 * Remove entire row.
	 * 
	 * @param colFamily		Name of the ColumnFamily from where row is removed.
	 * @param rowKey		RowKey of the row
	 */
	public void remove(String colFamily, TypedRow rowKey);
	
	public TypedRow find(String cf, Object id);
	
	public List<KeyValue<TypedRow>> findAllList(String colFamily, Iterable<Object> rowKeys);
	public Cursor<KeyValue<TypedRow>> createFindCursor(String colFamily, Iterable<Object> rowKeys, int batchSize);

	/**
	 * This creates a query cursor that will query into the database AS you iterate over the cursor.  You
	 * can also save the cursor to pick up where you left off.  
	 * 
	 * @param query			S-SQl query to be executed
	 * @param batchSize		Size of the batch
	 * @return <code>QueryResult</code> 
	 */
	public QueryResult createQueryCursor(String query, int batchSize);

	public int executeQuery(String query);
	
	public void flush();

	public Cursor<IndexPoint> indexView(String columnFamily, String column, String partitionBy, String partitionId);

	public void removeIndexPoint(IndexPoint pt, String partitionBy, String partitionId);
	public void addIndexPoint(IndexPoint pt, String partitionBy, String partitionId);

	public int count(String columnFamily, String indexedColName, Object value);

	public Cursor<TimeValColumn> timeSeriesSlice(String colFamily, BigInteger rowKey, long start, long end, int batchSize);

}
