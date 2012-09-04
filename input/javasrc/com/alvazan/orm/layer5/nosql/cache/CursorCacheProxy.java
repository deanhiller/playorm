package com.alvazan.orm.layer5.nosql.cache;

import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z8spi.AbstractCursor;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;

public class CursorCacheProxy extends AbstractCursor<KeyValue<Row>> {

	private List<RowHolder<Row>> rowsFromCacheIterable;

	private Iterator<RowHolder<Row>> rowsFromCache;
	private AbstractCursor<KeyValue<Row>> rowsFromDb;
	private NoSqlReadCacheImpl cache;
	private String colFamily;

	public CursorCacheProxy(NoSqlReadCacheImpl cache, String colFamily, List<RowHolder<Row>> rows, AbstractCursor<KeyValue<Row>> rowsFromDb) {
		this.cache = cache;
		this.colFamily = colFamily;
		rowsFromCacheIterable = rows;
		this.rowsFromDb = rowsFromDb;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rowsFromCache = rowsFromCacheIterable.iterator();
		rowsFromDb.beforeFirst();
	}

	@Override
	public Holder<KeyValue<Row>> nextImpl() {
		if(!rowsFromCache.hasNext())
			return null;
		
		//BASICALLY, we previously created a rowsFromCache that has all cached rows AND
		//null values in the spots where rows were not found.
		RowHolder<Row> cachedRow = rowsFromCache.next();
		if(cachedRow != null) {
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(cachedRow.getKey());
			kv.setValue(cachedRow.getValue());
			return new Holder<KeyValue<Row>>(kv);
		}
		
		KeyValue<Row> kv = rowsFromDb.next();
		cache.cacheRow(colFamily, (byte[]) kv.getKey(), kv.getValue());
		return new Holder<KeyValue<Row>>(kv);
	}
	
}
