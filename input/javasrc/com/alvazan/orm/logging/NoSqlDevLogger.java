package com.alvazan.orm.logging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

/**
 * WE need to use this to see when the proxies are accidentally going back to the cache of already loaded rows and RE-translating everything
 * they had already translated the first time.
 * @author dhiller2
 *
 */
public class NoSqlDevLogger implements NoSqlSession {

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
	public List<Row> find(String colFamily, List<byte[]> rowKeys) {
		NoSqlRawLogger.logKeys("[cache]", databaseInfo, colFamily, rowKeys);
		return session.find(colFamily, rowKeys);
	}

	@Override
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> keys = new ArrayList<byte[]>();
		keys.add(rowKey);
		NoSqlRawLogger.logKeys("[cache]", databaseInfo, colFamily, keys);
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
	public Iterable<Column> columnRangeScan(ScanInfo scanInfo, byte[] from, boolean fromInclusive, byte[] to, boolean toInclusive) {
		return session.columnRangeScan(scanInfo, from, fromInclusive, to, toInclusive);
	}

	@Override
	public void setOrmSessionForMeta(Object orm) {
		session.setOrmSessionForMeta(orm);
	}

	@Override
	public Iterable<Column> columnRangeScanAll(ScanInfo scanInfo) {
		return session.columnRangeScanAll(scanInfo);
	}

}
