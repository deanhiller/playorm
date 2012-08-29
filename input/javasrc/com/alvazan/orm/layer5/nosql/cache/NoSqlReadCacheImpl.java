package com.alvazan.orm.layer5.nosql.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

public class NoSqlReadCacheImpl implements NoSqlSession {

	@Inject @Named("writecachelayer")
	private NoSqlSession session;
	private Map<TheKey, RowHolder<Row>> cache = new HashMap<TheKey, RowHolder<Row>>();
	@Inject
	private Provider<Row> rowProvider;
	
	@Override
	public void put(String colFamily, byte[] rowKey, List<Column> columns) {
		session.put(colFamily, rowKey, columns);
		RowHolder<Row> currentRow = fromCache(colFamily, rowKey);
		if(currentRow == null) {
			currentRow = new RowHolder<Row>(rowKey);
		}

		Row value = currentRow.getValue();
		if(value == null)
			value = rowProvider.get();
		
		value.setKey(rowKey);
		value.addColumns(columns);
		cacheRow(colFamily, rowKey, value);
	}

	@Override
	public void persistIndex(String colFamily, String indexColFamily, byte[] rowKey, IndexColumn columns) {
		if(indexColFamily == null)
			throw new IllegalArgumentException("indexcolFamily cannot be null");
		else if(rowKey == null)
			throw new IllegalArgumentException("rowKey cannot be null");
		else if(columns == null)
			throw new IllegalArgumentException("column cannot be null");
		session.persistIndex(colFamily, indexColFamily, rowKey, columns);
	}
	
	@Override
	public void remove(String colFamily, byte[] rowKey) {
		session.remove(colFamily, rowKey);
		cacheRow(colFamily, rowKey, null);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey, Collection<byte[]> columnNames) {
		session.remove(colFamily, rowKey, columnNames);
		
		RowHolder<Row> currentRow = fromCache(colFamily, rowKey);
		if(currentRow == null) {
			return;
		}
		Row value = currentRow.getValue();
		if(value == null) {
			return;
		}
		
		value.removeColumns(columnNames);
	}

	@Override
	public Row find(String colFamily, byte[] rowKey) {
		RowHolder<Row> result = fromCache(colFamily, rowKey);
		if(result != null)
			return result.getValue(); //This may return the cached null value!!
		
		Row row = session.find(colFamily, rowKey);
		cacheRow(colFamily, rowKey, row);
		return row;
	}
	
	@Override
	public Iterable<KeyValue<Row>> findAll(String colFamily, Iterable<byte[]> rowKeys, boolean skipCache) {
		if(skipCache) {
			return session.findAll(colFamily, rowKeys, skipCache);
		}
		
		IterCacheKeysProxy proxy = new IterCacheKeysProxy(this, colFamily, rowKeys);
		Iterable<KeyValue<Row>> rowsFromDb = session.findAll(colFamily, proxy, skipCache);
		//NOW we must MERGE both Iterables back together!!! as IterCacheKeysProxy looked up
		//existing rows
		List<RowHolder<Row>> inCache = proxy.getFoundInCache();
		Iterable<KeyValue<Row>> theRows = new IterCacheProxy(this, colFamily, inCache, rowsFromDb);
		return theRows;
	}
	
	RowHolder<Row> fromCache(String colFamily, byte[] key) {
		TheKey k = new TheKey(colFamily, key);
		return cache.get(k);
	}

	void cacheRow(String colFamily, byte[] key, Row r) {
		//NOTE: Do we want to change Map<TheKey, Row> to Map<TheKey, Holder<Row>> so we can cache null rows?
		TheKey k = new TheKey(colFamily, key);
		RowHolder<Row> holder = new RowHolder<Row>(key, r); //r may be null so we are caching null here
		cache.put(k, holder);
	}

	static class TheKey {
		private String colFamily;
		private ByteArray key;
		
		public TheKey(String cf, byte[] key) {
			colFamily = cf;
			this.key = new ByteArray(key);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((colFamily == null) ? 0 : colFamily.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TheKey other = (TheKey) obj;
			if (colFamily == null) {
				if (other.colFamily != null)
					return false;
			} else if (!colFamily.equals(other.colFamily))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}
		
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
	public void removeFromIndex(String cf, String indexColFamily, byte[] rowKeyBytes,
			IndexColumn c) {
		session.removeFromIndex(cf, indexColFamily, rowKeyBytes, c);
	}
	
	@Override
	public void clearDb() {
		session.clearDb();
	}

	@Override
	public Iterable<Column> columnSlice(String colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize) {
		return session.columnSlice(colFamily, rowKey, from, to, batchSize);
	}
	
	@Override
	public Iterable<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return session.scanIndex(info, from, to, batchSize);
	}

	@Override
	public void setOrmSessionForMeta(Object ormSession) {
		session.setOrmSessionForMeta(ormSession);
	}

}
