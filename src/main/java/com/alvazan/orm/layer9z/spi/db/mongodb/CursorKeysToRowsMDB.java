package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class CursorKeysToRowsMDB extends AbstractCursor<KeyValue<Row>> {

	private DirectCursor<byte[]> rowKeys;
	private BatchListener list;
	private ListIterator<KeyValue<Row>> cachedRows;
	private Cache cache;
	private DboTableMeta colFamily;
	private DB database;

	public CursorKeysToRowsMDB(DboTableMeta colFamily, DirectCursor<byte[]> rowKeys, BatchListener list, DB database, Cache cache2) {
		this.colFamily = colFamily;
		this.cache = cache2;
		this.database = database;
		this.rowKeys = rowKeys;
		this.list = list;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rowKeys.beforeFirst();
		cachedRows = null;
	}
	
	@Override
	public void afterLast() {
		rowKeys.afterLast();
		cachedRows = null;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		loadCache();
		if(cachedRows == null || !cachedRows.hasNext())
			return null;
		
		return new Holder<KeyValue<Row>>(cachedRows.next());
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> previousImpl() {
		loadCacheBackward();
		if(cachedRows == null || !cachedRows.hasPrevious())
			return null;
		
		return new Holder<KeyValue<Row>>(cachedRows.previous());
	}

	private void loadCache() {
		byte[] nextKey = null;
		Holder<byte[]> keyHolder = rowKeys.nextImpl();
		if (keyHolder != null)
			nextKey = keyHolder.getValue();
		
		if(cachedRows != null && cachedRows.hasNext())
			return; //There are more rows so return and the code will return the next result from cache
		else if(nextKey == null)
			return;
		
		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while(nextKey != null) {
			RowHolder<Row> result = cache.fromCache(colFamily, nextKey);
			if(result == null)
				keysToLookup.add(nextKey);
			
			results.add(result);
			nextKey = null;
			keyHolder = rowKeys.nextImpl();
			if (keyHolder != null)
				nextKey = keyHolder.getValue();
		}
		
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		if(keysToLookup.size() > 0) {
			if(list != null)
				list.beforeFetchingNextBatch();
			rows = fetchRows();
			if(list != null)
				list.afterFetchingNextBatch(rows.size());
		}

		Iterator<KeyValue<Row>> resultingRows = rows.iterator();
		
		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while(resultingRows.hasNext()) {
			KeyValue<Row> kv = resultingRows.next();			
			byte[] key = (byte[]) kv.getKey();
			ByteArray b = new ByteArray(key);
			map.put(b, kv);
			cache.cacheRow(colFamily, key, kv.getValue());
		}
		
		//UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we provided so, we need to iterate over the whole thing here
		//into our own List :( :( .....okay, well, we are now in memory and would have been ordered but oh well....rework this later

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
		
		cachedRows = finalRes.listIterator();
	}
	
	
	private void loadCacheBackward() {
		byte[] previousKey = null;
		Holder<byte[]> keyHolder = rowKeys.previousImpl();
		if (keyHolder != null)
			previousKey = keyHolder.getValue();
		
		if(cachedRows != null && cachedRows.hasPrevious())
			return; //There are more rows so return and the code will return the next result from cache
		else if(previousKey == null)
			return;
		
		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		ArrayList<byte[]> keysToLookup = new ArrayList<byte[]>();
		while(previousKey != null) {
			RowHolder<Row> result = cache.fromCache(colFamily, previousKey);
			if(result == null)
				keysToLookup.add(0, previousKey);
			
			results.add(result);
			previousKey = null;
			keyHolder = rowKeys.previousImpl();
			if (keyHolder != null)
				previousKey = keyHolder.getValue();
		}
		
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		if(keysToLookup.size() > 0) {
			if(list != null)
				list.beforeFetchingNextBatch();
			rows = fetchRowsBackward();
			if(list != null)
				list.afterFetchingNextBatch(rows.size());
		}

		Iterator<KeyValue<Row>> resultingRows = rows.iterator();
		
		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while(resultingRows.hasNext()) {
			KeyValue<Row> kv = resultingRows.next();			
			byte[] key = (byte[]) kv.getKey();
			ByteArray b = new ByteArray(key);
			map.put(b, kv);
			cache.cacheRow(colFamily, key, kv.getValue());
		}
		
		//UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we provided so, we need to iterate over the whole thing here
		//into our own List :( :( .....okay, well, we are now in memory and would have been ordered but oh well....rework this later

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
		
		cachedRows = finalRes.listIterator();
		while (cachedRows.hasNext()) cachedRows.next();
	}

	

	public List<KeyValue<Row>> fetchRows() {
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		DBCollection table = database.getCollection(colFamily.getColumnFamily());
		rowKeys.beforeFirst();
		Holder<byte[]> keyHolder = rowKeys.nextImpl();
		if (keyHolder != null) {
			byte[] key = keyHolder.getValue();
			while(key != null) {
				Row row = findRow(table, key);
				Row newRow = null;
				if(row != null)
					newRow = row.deepCopy();
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(key);
				kv.setValue(newRow);
				//This add null if there is no row to the list on purpose
				rows.add(kv);
				keyHolder = rowKeys.nextImpl();
				key = null;
				if (keyHolder != null)
					key = keyHolder.getValue();
			}
		}
		return rows;
	}
	
	public List<KeyValue<Row>> fetchRowsBackward() {
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		DBCollection table = database.getCollection(colFamily.getColumnFamily());
		rowKeys.afterLast();
		Holder<byte[]> keyHolder = rowKeys.previousImpl();
		if (keyHolder != null) {
			byte[] key = keyHolder.getValue();
			while(key != null) {
				Row row = findRow(table, key);
				Row newRow = null;
				if(row != null)
					newRow = row.deepCopy();
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(key);
				kv.setValue(newRow);
				//This add null if there is no row to the list on purpose
				rows.add(0, kv);
				keyHolder = rowKeys.previousImpl();
				key = null;
				if (keyHolder != null)
					key = keyHolder.getValue();
			}
		}
		return rows;
	}
	
	private Row findRow(DBCollection table, byte[] key) {
		if(table == null)
			return null;
		//return table.getRow(key);
		return null;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = ""+rowKeys;
		if(rowKeys instanceof List)
			keys = "List"+keys;
		String retVal = "CursorKeysToRows(inmemoryRowLookup)["+tabs+keys+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
}
