package com.alvazan.orm.layer2.nosql;

import java.util.List;

import com.alvazan.orm.layer2.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer3.spi.Column;
import com.alvazan.orm.layer3.spi.NoSqlRawSession;
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
	
	public void persist(String colFamily, byte[] rowKey, List<Column> columns);

	/**
	 * Remove entire row.
	 * 
	 * @param colFamily
	 * @param rowKey
	 */
	public void remove(String colFamily, byte[] rowKey);
	
	/**
	 * Remove specific columns from a row
	 * 
	 * @param colFamily
	 * @param rowKey
	 * @param columns
	 */
	public void remove(String colFamily, byte[] rowKey, List<String> columnNames);
	
	public List<Row> find(String colFamily, List<byte[]> rowKeys);
	
	public void flush();

}
