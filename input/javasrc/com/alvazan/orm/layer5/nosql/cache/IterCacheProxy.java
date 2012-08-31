package com.alvazan.orm.layer5.nosql.cache;

import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;

public class IterCacheProxy implements Iterable<KeyValue<Row>> {

	private List<RowHolder<Row>> rowsFromCache;
	private Iterable<KeyValue<Row>> rowsFromDb;
	private NoSqlReadCacheImpl cache;
	private String colFamily;

	public IterCacheProxy(NoSqlReadCacheImpl cache, String colFamily, List<RowHolder<Row>> rows, Iterable<KeyValue<Row>> rowsFromDb) {
		this.cache = cache;
		this.colFamily = colFamily;
		rowsFromCache = rows;
		this.rowsFromDb = rowsFromDb;
	}

	@Override
	public Iterator<KeyValue<Row>> iterator() {
		return new IterableCacheProxy(cache, colFamily, rowsFromCache.iterator(), rowsFromDb.iterator());
	}

	private static class IterableCacheProxy implements Iterator<KeyValue<Row>> {

		private Iterator<RowHolder<Row>> rowsFromCache;
		private Iterator<KeyValue<Row>> rowsFromDb;
		private NoSqlReadCacheImpl cache;
		private String colFamily;

		public IterableCacheProxy(NoSqlReadCacheImpl cache, String colFamily, Iterator<RowHolder<Row>> rowsFromCache,
				Iterator<KeyValue<Row>> fromDb) {
			this.cache = cache;
			this.colFamily = colFamily;
			this.rowsFromCache = rowsFromCache;
			this.rowsFromDb = fromDb;
		}

		@Override
		public boolean hasNext() {
			boolean has = rowsFromCache.hasNext();
			return has;
		}

		@Override
		public KeyValue<Row> next() {
			//BASICALLY, we previously created a rowsFromCache that has all cached rows AND
			//null values in the spots where rows were not found.
			RowHolder<Row> cachedRow = rowsFromCache.next();
			if(cachedRow != null) {
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(cachedRow.getKey());
				kv.setValue(cachedRow.getValue());
				return kv;
			}
			
			KeyValue<Row> kv = rowsFromDb.next();
			cache.cacheRow(colFamily, (byte[]) kv.getKey(), kv.getValue());
			return kv;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported, never will be");
		}
	}
}
