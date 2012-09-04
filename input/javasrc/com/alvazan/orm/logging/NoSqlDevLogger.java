package com.alvazan.orm.logging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.AbstractCursor;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;

/**
 * WE need to use this to see when the proxies are accidentally going back to the cache of already loaded rows and RE-translating everything
 * they had already translated the first time.
 * @author dhiller2
 *
 */
public class NoSqlDevLogger implements NoSqlSession {

	private static final Logger log = LoggerFactory.getLogger(NoSqlDevLogger.class);
	
	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	@Inject
	private DboDatabaseMeta databaseInfo;
	
	@Override
	public NoSqlRawSession getRawSession() {
		return session.getRawSession();
	}

	@Override
	public void persistIndex(String colFamily, String indexColFamily,
			byte[] rowKey, IndexColumn column) {
		session.persistIndex(colFamily, indexColFamily, rowKey, column);
	}

	@Override
	public void removeFromIndex(String colFamily, String indexColFamily,
			byte[] rowKeyBytes, IndexColumn c) {
		session.removeFromIndex(colFamily, indexColFamily, rowKeyBytes, c);
	}

	@Override
	public void put(String colFamily, byte[] rowKey, List<Column> columns) {
		session.put(colFamily, rowKey, columns);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey) {
		session.remove(colFamily, rowKey);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey,
			Collection<byte[]> columnNames) {
		session.remove(colFamily, rowKey, columnNames);
	}

	@Override
	public AbstractCursor<KeyValue<Row>> findAll(String colFamily, Iterable<byte[]> rowKeys, boolean skipCache) {
		log.warn("CAN't use this method as it would cause the iterable to loop and we only want to loop ONCE so need a proxy iterable to pass down!!!!");
		//NoSqlRawLogger.logKeys("[cache]", databaseInfo, colFamily, rowKeys);
		return session.findAll(colFamily, rowKeys, skipCache);
	}
	
	@Override
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> keys = new ArrayList<byte[]>();
		keys.add(rowKey);
		//NoSqlRawLogger.logKeys("[cache]", databaseInfo, colFamily, keys);
		return session.find(colFamily, rowKey);
	}

	@Override
	public void flush() {
		session.flush();
	}

	@Override
	public void clearDb() {
		session.clearDb();
	}

	@Override
	public AbstractCursor<Column> columnSlice(String colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize) {
		return session.columnSlice(colFamily, rowKey, from, to, batchSize);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return session.scanIndex(info, from, to, batchSize);
	}
	
	@Override
	public void setOrmSessionForMeta(MetaLookup orm) {
		session.setOrmSessionForMeta(orm);
	}


}
