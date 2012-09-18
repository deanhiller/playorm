package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;

public class CursorKeysToRows extends AbstractCursor<KeyValue<Row>> {

	private Iterable<byte[]> rowKeys;
	private Iterator<byte[]> theKeys;
	private BatchListener list;
	private Iterator<KeyValue<Row>> cachedRows;
	private Cache cache;
	private String colFamily;
	private NoSqlDatabase database;

	public CursorKeysToRows(String colFamily, Iterable<byte[]> rowKeys, BatchListener list, NoSqlDatabase database, Cache cache2) {
		this.colFamily = colFamily;
		this.cache = cache2;
		this.database = database;
		this.rowKeys = rowKeys;
		this.list = list;
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

	private void loadCache() {
		if(cachedRows != null && cachedRows.hasNext())
			return; //There are more rows so return and the code will return the next result from cache
		else if(!theKeys.hasNext())
			return;
		
		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while(theKeys.hasNext()) {
			byte[] key = theKeys.next();
			RowHolder<Row> result = cache.fromCache(colFamily, key);
			if(result == null)
				keysToLookup.add(key);
			
			results.add(result);
		}
		
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		if(keysToLookup.size() > 0) {
			list.beforeFetchingNextBatch();
			rows = fetchRows();
			list.afterFetchingNextBatch(rows.size());
		}

		Iterator<KeyValue<Row>> resultingRows = rows.iterator();
		
		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while(resultingRows.hasNext()) {
			KeyValue<Row> kv = resultingRows.next();			
			ByteArray b = new ByteArray((byte[]) kv.getKey());
			map.put(b, kv);
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
		
		cachedRows = finalRes.iterator();
	}

	public List<KeyValue<Row>> fetchRows() {
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		Table table = database.findTable(colFamily);
		for(byte[] key : rowKeys) {
			Row row = findRow(table, key);
			Row newRow = null;
			if(row != null)
				newRow = row.deepCopy();
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(key);
			kv.setValue(newRow);
			//This add null if there is no row to the list on purpose
			rows.add(kv);
		}
		return rows;
	}
	private Row findRow(Table table, byte[] key) {
		if(table == null)
			return null;
		return table.getRow(key);
	}
	
}
