package com.alvazan.orm.layer2.nosql.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.ColumnType;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.index.IndexReaderWriter;

public class NoSqlReadCacheImpl implements NoSqlSession {

	@Inject @Named("writecachelayer")
	private NoSqlSession session;
	
	@Override
	public void persist(String colFamily, byte[] rowKey, List<Column> columns) {
		session.persist(colFamily, rowKey, columns);
	}

	@Override
	public void persistIndex(String colFamily, byte[] rowKey, IndexColumn columns, ColumnType type) {
		session.persistIndex(colFamily, rowKey, columns, type);
	}
	
	@Override
	public void remove(String colFamily, byte[] rowKey) {
		session.remove(colFamily, rowKey);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey, Collection<byte[]> columnNames) {
		session.remove(colFamily, rowKey, columnNames);
	}

	@Override
	public List<Row> find(String colFamily, List<byte[]> rowKeys) {
		//READ FROM CACHE HERE to skip reading from database.
		//All Proxies read from this find method too so they can get cache hits when you have a large 
		//object graph and fill themselves in properly
		List<Row> rows = session.find(colFamily, rowKeys);
		//We can cache stuff here....
		cacheRows(rows);
		return rows;
	}
	
	private void cacheRows(List<Row> rows) {
	}

	@Override
	public void flush() {
		session.flush();
	}

	@Override
	public NoSqlRawSession getRawSession() {
		return session.getRawSession();
	}

	@Override
	public void removeFromIndex(String columnFamilyName, byte[] rowKeyBytes,
			IndexColumn c) {
		session.removeFromIndex(columnFamilyName, rowKeyBytes, c);
	}
	
	@Override
	public IndexReaderWriter getRawIndex() {
		return session.getRawIndex();
	}

	@Override
	public void clearDbAndIndexesIfInMemoryType() {
		session.clearDbAndIndexesIfInMemoryType();
	}

	@Override
	public Iterable<Column> columnRangeScan(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) {
		return session.columnRangeScan(colFamily, rowKey, from, to, batchSize);
	}

	@Override
	public void setOrmSessionForMeta(Object ormSession) {
		session.setOrmSessionForMeta(ormSession);
	}

}
