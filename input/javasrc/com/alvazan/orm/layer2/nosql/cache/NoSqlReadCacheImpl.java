package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi1.meta.conv.ByteArray;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Row;

public class NoSqlReadCacheImpl implements NoSqlSession {

	private static final Logger log = LoggerFactory.getLogger(NoSqlReadCacheImpl.class);
	
	@Inject @Named("writecachelayer")
	private NoSqlSession session;
	private Map<TheKey, RowHolder<Row>> cache = new HashMap<TheKey, RowHolder<Row>>();
			
	@Override
	public void put(String colFamily, byte[] rowKey, List<Column> columns) {
		session.put(colFamily, rowKey, columns);
	}

	@Override
	public void persistIndex(String colFamily, byte[] rowKey, IndexColumn columns) {
		session.persistIndex(colFamily, rowKey, columns);
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
	public Row find(String colFamily, byte[] rowKey) {
		RowHolder<Row> result = fromCache(colFamily, rowKey);
		if(result != null)
			return result.getValue(); //This may return the cached null value!!
		
		Row row = session.find(colFamily, rowKey);
		cacheRow(colFamily, rowKey, row);
		return row;
	}
	
	@Override
	public List<Row> find(String colFamily, List<byte[]> rowKeys) {
		//READ FROM CACHE HERE to skip reading from database.
		//All Proxies read from this find method too so they can get cache hits when you have a large 
		//object graph and fill themselves in properly
		List<Row> rows = new ArrayList<Row>();
		List<byte[]> rowKeysToFetch = new ArrayList<byte[]>();
		List<Integer> indexForRow = new ArrayList<Integer>();
		for(int i = 0; i < rowKeys.size();i++) {
			byte[] key = rowKeys.get(i);
			RowHolder<Row> result = fromCache(colFamily, key);
			if(result == null) {
				indexForRow.add(i);
				rowKeysToFetch.add(key);
				//we still add the null result that will be replaced after we
				//hit the database with rows that are still needed..
				rows.add(null);
			} else {
				log.info("cache hit on k="+new ByteArray(key));
				rows.add(result.getValue());
			}
		}
		
		List<Row> rowsFromDb = session.find(colFamily, rowKeysToFetch);
		
		for(int i = 0; i < rowKeysToFetch.size(); i++) {
			Integer index = indexForRow.get(i);
			Row r = rowsFromDb.get(i);
			byte[] key = rowKeysToFetch.get(i);
			rows.set(index, r);
			cacheRow(colFamily, key, r);
		}
		
		return rows;
	}
	
	private RowHolder<Row> fromCache(String colFamily, byte[] key) {
		TheKey k = new TheKey(colFamily, key);
		return cache.get(k);
	}

	private void cacheRow(String colFamily, byte[] key, Row r) {
		//NOTE: Do we want to change Map<TheKey, Row> to Map<TheKey, Holder<Row>> so we can cache null rows?
		TheKey k = new TheKey(colFamily, key);
		RowHolder<Row> holder = new RowHolder<Row>(r); //r may be null so we are caching null here
		cache.put(k, holder);
	}

	private static class TheKey {
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
	public void removeFromIndex(String columnFamilyName, byte[] rowKeyBytes,
			IndexColumn c) {
		session.removeFromIndex(columnFamilyName, rowKeyBytes, c);
	}
	
	@Override
	public void clearDb() {
		session.clearDb();
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
