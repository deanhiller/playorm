package com.alvazan.orm.api.spi1;

import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.TypedRow;
import com.alvazan.orm.api.spi3.db.Column;

@SuppressWarnings("rawtypes")
public interface NoSqlTypedSession {

	@Deprecated
	public void setRawSession(NoSqlSession s);
	@Deprecated
	public void setMetaInfo(DboDatabaseMeta meta);
	
	/**
	 * Retrieves the rawest interface that all the providers implement(in-memory, cassandra, hadoop, etc) BUT
	 * be warned, when you call persist, it is executed IMMEDIATELY, there is no flush method on that
	 * interface.  In fact, the flush methods of all higher level interfaces call persist ONCE on the raw
	 * session and providers are supposed to do ONE send to avoid network latency if you are sending 100
	 * actions to the nosql database.
	 * @return
	 */
	public NoSqlSession getRawSession();

	/**
	 * This api takes BigDecimal, BigInteger, or String as the types and no others for the rowKey
	 * @param colFamily
	 * @param rowKey rowkey which is BigDecimal, BigInteger, or String
	 * @param columns
	 */
	public void put(String colFamily, TypedRow row);
	
	/**
	 * Remove entire row.
	 * 
	 * @param colFamily
	 * @param rowKey
	 */
	public void remove(String colFamily, TypedRow rowKey);
	
	/**
	 * Remove specific columns from a row
	 * 
	 * @param colFamily
	 * @param rowKey
	 * @param columns
	 */
	public <T> void remove(String colFamily, T rowKey, Collection<byte[]> columnNames);
	
	public <T> List<TypedRow<T>> find(String colFamily, List<T> rowKeys);
	
	public void flush();

	public void clearDatabase();

	/**
	 * Returns a special Iterable loads itself first with "batchSize" records and then after you
	 * iterator over those and request "batchSize+1" record, it hits the database again such that
	 * your memory does not explode if you have a huge amount of records.
	 * 
	 * @param colFamily
	 * @param rowKey
	 * @param from
	 * @param to
	 * @param batchSize
	 * @return
	 */
	public Iterable<Column> columnRangeScan(String colFamily, Object rowKey,
			Object from, Object to, int batchSize);

	public void setOrmSessionForMeta(Object session);
}
