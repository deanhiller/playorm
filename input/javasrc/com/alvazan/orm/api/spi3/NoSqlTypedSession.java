package com.alvazan.orm.api.spi3;

import java.util.List;

import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.layer3.typed.NoSqlTypedSessionImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlTypedSessionImpl.class)
@SuppressWarnings("rawtypes")
public interface NoSqlTypedSession {
	
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
	
	public <T> TypedRow<T> find(String cf, T id);
	
	public <T> List<KeyValue<TypedRow<T>>> findAll(String colFamily, List<T> rowKeys);

	public List<KeyValue<TypedRow>> runQuery(String query);
	
	public void flush();

}
