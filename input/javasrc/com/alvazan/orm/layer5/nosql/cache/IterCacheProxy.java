package com.alvazan.orm.layer5.nosql.cache;

import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;

public class IterCacheProxy implements Iterable<KeyValue<Row>> {

	private List<RowHolder<Row>> rowsFromCache;
	private Iterable<KeyValue<Row>> rowsFromDb;

	public IterCacheProxy(List<RowHolder<Row>> rows, List<Integer> indexForRow,
			Iterable<KeyValue<Row>> rowsFromDb) {
		rowsFromCache = rows;
		this.rowsFromDb = rowsFromDb;
	}

	@Override
	public Iterator<KeyValue<Row>> iterator() {
		return new IterableCacheProxy(rowsFromCache.iterator(), rowsFromDb.iterator());
	}

	private static class IterableCacheProxy implements Iterator<KeyValue<Row>> {

		private Iterator<RowHolder<Row>> rowsFromCache;
		private Iterator<KeyValue<Row>> rowsFromDb;

		public IterableCacheProxy(Iterator<RowHolder<Row>> fromCache,
				Iterator<KeyValue<Row>> fromDb) {
			this.rowsFromCache = fromCache;
			this.rowsFromDb = fromDb;
		}

		@Override
		public boolean hasNext() {
			return rowsFromCache.hasNext();
		}

		@Override
		public KeyValue<Row> next() {
			RowHolder<Row> cachedRow = rowsFromCache.next();
			if(cachedRow != null) {
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(cachedRow.getKey());
				kv.setValue(cachedRow.getValue());
				return kv;
			}
			
			return rowsFromDb.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported, never will be");
		}
	}
}
