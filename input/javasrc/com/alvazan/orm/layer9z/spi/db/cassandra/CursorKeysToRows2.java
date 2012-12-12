package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;

public class CursorKeysToRows2 extends AbstractCursor<KeyValue<Row>> {

	private Info info;
	private Iterable<byte[]> rowKeys;
	private int batchSize;
	private BatchListener list;
	private Keyspace keyspace;
	private Iterator<byte[]> theKeys;
	private Iterator<KeyValue<Row>> cachedRows;
	private Provider<Row> rowProvider;
	private Cache cache;
	private DboTableMeta cf;

	public CursorKeysToRows2(Iterable<byte[]> rowKeys, int batchSize,
			BatchListener list, Provider<Row> rowProvider) {
		this.rowProvider = rowProvider;
		this.rowKeys = rowKeys;
		this.batchSize = batchSize;
		this.list = list;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = ""+rowKeys;
		if(rowKeys instanceof List)
			keys = "List"+keys;
		String retVal = "CursorKeysToRows2(cassandraFindRows)["+tabs+keys+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	public void setupMore(Keyspace keyspace, DboTableMeta cf, Info info, Cache cache) {
		if(cache == null || keyspace == null || cf == null | info == null)
			throw new IllegalArgumentException("no params can be null but one was null");
		this.cf = cf;
		this.info = info;
		this.cache = cache;
		this.keyspace = keyspace;
		beforeFirst();
	}
	@Override
	public void beforeFirst() {
		theKeys = rowKeys.iterator();
		cachedRows = null;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		loadCache();
		if(cachedRows == null || !cachedRows.hasNext())
			return null;
		
		return new Holder<KeyValue<Row>>(cachedRows.next());
	}

	@SuppressWarnings("unchecked")
	private void loadCache() {
		if(cachedRows != null && cachedRows.hasNext())
			return; //There are more rows so return and the code will return the next result from cache
		else if(!theKeys.hasNext())
			return;
		
		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while(results.size() < batchSize && theKeys.hasNext()) {
			byte[] key = theKeys.next();
			RowHolder<Row> result = cache.fromCache(cf, key);
			if(result == null)
				keysToLookup.add(key);
			
			results.add(result);
		}

		
		Iterator<com.netflix.astyanax.model.Row<byte[], byte[]>> resultingRows = null;
		if(keysToLookup.size() > 0) {
			if(list != null)
				list.beforeFetchingNextBatch();
			
			ColumnFamily<byte[], byte[]> cf = info.getColumnFamilyObj();
			ColumnFamilyQuery<byte[], byte[]> q2 = keyspace.prepareQuery(cf);
			RowSliceQuery<byte[], byte[]> slice = q2.getKeySlice(keysToLookup);
			
			OperationResult<Rows<byte[], byte[]>> result = execute(slice);
			
			Rows<byte[], byte[]> rows = result.getResult();		
			resultingRows = rows.iterator();
			if(list != null)
				list.afterFetchingNextBatch(rows.size());
		} else {
			resultingRows = new ArrayList<com.netflix.astyanax.model.Row<byte[], byte[]>>().iterator();
		}

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while(resultingRows.hasNext()) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = resultingRows.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(row.getKey());
			if(!row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				Row r = rowProvider.get();
				r.setKey(row.getKey());
				CassandraSession.processColumns(row, r);
				kv.setValue(r);
			}
			
			ByteArray b = new ByteArray(row.getKey());
			map.put(b, kv);
			cache.cacheRow(cf, row.getKey(), kv.getValue());
		}
		
		//UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we provided so, we need to iterate over the whole thing here
		//into our own List :( :( .

		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for(RowHolder<Row> r : results) {
			if(r == null) {
				byte[] key = keyIter.next();
				ByteArray b = new ByteArray(key);
				KeyValue<Row> kv = map.get(b);
				finalRes.add(kv);				
			} else {
				Row row = r.getValue();
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(r.getKey());
				kv.setValue(row);
				finalRes.add(kv);
			}
		}
		
		cachedRows = finalRes.iterator();
	}

	private OperationResult<Rows<byte[], byte[]>> execute(
			RowSliceQuery<byte[], byte[]> slice) {
		try {
			return slice.execute();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

}
