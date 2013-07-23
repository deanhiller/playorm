package com.alvazan.orm.logging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

/**
 * WE need to use this to see when the proxies are accidentally going back to the cache of already loaded rows and RE-translating everything
 * they had already translated the first time.
 * @author dhiller2
 *
 */
public class NoSqlDevLogger implements NoSqlSession {

	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	
	@Override
	public NoSqlRawSession getRawSession() {
		return session.getRawSession();
	}

	@Override
	public void persistIndex(DboTableMeta colFamily, String indexColFamily, byte[] rowKey, IndexColumn column) {
		session.persistIndex(colFamily, indexColFamily, rowKey, column);
	}

	@Override
	public void removeFromIndex(DboTableMeta colFamily, String indexColFamily,
			byte[] rowKeyBytes, IndexColumn c) {
		session.removeFromIndex(colFamily, indexColFamily, rowKeyBytes, c);
	}

	@Override
	public void put(DboTableMeta colFamily, byte[] rowKey, List<Column> columns) {
		session.put(colFamily, rowKey, columns);
	}

	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey) {
		session.remove(colFamily, rowKey);
	}

	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey,
			Collection<byte[]> columnNames) {
		session.remove(colFamily, rowKey, columnNames);
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta cf,
			DirectCursor<byte[]> noSqlKeys, boolean skipCache, boolean cacheResults, Integer batchSize) {
		return session.find(cf, noSqlKeys, skipCache, cacheResults, batchSize);
	}
	
	@Override
	public Row find(DboTableMeta colFamily, byte[] rowKey) {
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
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize, Class columnNameType) {
		return session.columnSlice(colFamily, rowKey, from, to, batchSize, columnNameType);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return session.scanIndex(info, from, to, batchSize);
	}
	
	@Override
	public void setOrmSessionForMeta(MetaLookup orm) {
		session.setOrmSessionForMeta(orm);
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values) {
		return session.scanIndex(scanInfo, values);
	}

	@Override
	public void clear() {
	}

	@Override
	public void removeColumn(DboTableMeta colFamily, byte[] rowKey,
			byte[] columnName) {
		session.removeColumn(colFamily, rowKey, columnName);
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, int batchSize) {
		return session.allRows(colFamily, batchSize);
	}

}
