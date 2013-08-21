package com.alvazan.orm.layer9z.spi.db.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;


import javax.inject.Provider;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;


import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;



public class CursorKeysToRowsHbase extends AbstractCursor<KeyValue<Row>> {

	private DirectCursor<byte[]> rowKeys;
	private int batchSize;
	private BatchListener list;
	private HTableInterface hTable;
	private Info info;
	private ListIterator<KeyValue<Row>> cachedRows;
	private Provider<Row> rowProvider;
	private Cache cache;
	private DboTableMeta cf;

	public CursorKeysToRowsHbase(DirectCursor<byte[]> rowKeys, int batchSize,
			BatchListener list, Provider<Row> rowProvider) {
		this.rowProvider = rowProvider;
		this.rowKeys = rowKeys;
		this.batchSize = batchSize;
		this.list = list;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKeys;
		if (rowKeys instanceof List)
			keys = "List" + keys;
		String retVal = "CursorKeysToRowsHbase[" + tabs + keys
				+ tabs + "]";
		StringLocal.set(tabs.length());
		return retVal; 
	}

	public void setupMore(HTableInterface keyspace, DboTableMeta colFamily, Info info, Cache cache) {
		if (cache == null || keyspace == null || info == null){
			throw new IllegalArgumentException(
					"no params can be null but one was null");}
		this.cache = cache;
		this.hTable = keyspace;
		this.info = info;
		this.cf = colFamily;
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
		if (cachedRows == null || !cachedRows.hasNext())
			return null;

		return new Holder<KeyValue<Row>>(cachedRows.next());
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> previousImpl() {
		loadCacheBackward();
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;

		return new Holder<KeyValue<Row>>(cachedRows.previous());
	}

	private void loadCache() {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache

		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while (results.size() < batchSize) {
			Holder<byte[]> keyHolder = rowKeys.nextImpl();
			if (keyHolder == null)
				break; // we are officially exhausted

			byte[] nextKey = keyHolder.getValue();
			if (cache != null) {
				RowHolder<Row> result = cache.fromCache(cf, nextKey);
				if (result == null)
					keysToLookup.add(nextKey);
				results.add(result);
			}

		}
		Result[] resultArray = null;
		HColumnDescriptor dbCollection = null;
		if (info.getColFamily() != null) {
			dbCollection = info.getColFamily();
		} else
			return;
		if (keysToLookup.size() > 0) {
			if (list != null)
				list.beforeFetchingNextBatch();
			List<Get> listGet = new ArrayList<Get>();
			for (byte[] keys : keysToLookup) {
				Get get = new Get(keys);
				get.addFamily(dbCollection.getName());
				listGet.add(get);
			}
			try {
				resultArray = hTable.get(listGet);

			} catch (IOException e) {
				e.printStackTrace();
			}

			
			if (list != null)
				list.afterFetchingNextBatch(5);
		}

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		fillCache(map, resultArray, keysToLookup);

		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
				byte[] key = keyIter.next();
				ByteArray b = new ByteArray(key);
				KeyValue<Row> kv = map.get(b);
				if (kv != null)
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
		if (cachedRows != null && cachedRows.hasPrevious())
			return; // There are more rows so return and the code will return
					// the next result from cache

		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while (results.size() < batchSize) {
			Holder<byte[]> keyHolder = rowKeys.previousImpl();
			if (keyHolder == null)
				break; // we are officially exhausted

			byte[] previousKey = keyHolder.getValue();
			RowHolder<Row> result = cache.fromCache(cf, previousKey);
			if (result == null)
				keysToLookup.add(0, previousKey);

			results.add(result);
		}
		Result[] resultArray = null;
		HColumnDescriptor dbCollection = null;
		if (info.getColFamily() != null) {
			dbCollection = info.getColFamily();
		} else
			return;
		if (keysToLookup.size() > 0) {
			if (list != null)
				list.beforeFetchingNextBatch();
			List<Get> listGet = new ArrayList<Get>();
			for (byte[] keys : keysToLookup) {
				Get get = new Get(keys);
				get.addFamily(dbCollection.getName());
				listGet.add(get);
			}
			try {
				resultArray = hTable.get(listGet);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (list != null)
				list.afterFetchingNextBatch(5);
		}

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();

		fillCache(map, resultArray, keysToLookup);
		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
				byte[] key = keyIter.next();
				ByteArray b = new ByteArray(key);
				KeyValue<Row> kv = map.get(b);
				if (kv != null)
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
		while (cachedRows.hasNext())
			cachedRows.next();
	}

	private void fillCache(Map<ByteArray, KeyValue<Row>> map,
			Result[] resultArray, List<byte[]> keysToLookup) {
		if (resultArray == null) {
			for (byte[] key : keysToLookup) {
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(key);
				kv.setValue(null);
				ByteArray b = new ByteArray(key);
				map.put(b, kv);
				cache.cacheRow(cf, key, kv.getValue());
			}
		} else {
			for (Result result : resultArray) {
				List<org.apache.hadoop.hbase.KeyValue> hKeyValue = result.list();
				KeyValue<Row> kv = new KeyValue<Row>();
				if (hKeyValue!= null && !hKeyValue.isEmpty()) {
					kv.setKey(result.getRow());
					Row r = rowProvider.get();
					processColumns(hKeyValue, r);
					kv.setValue(r);
				}
				ByteArray b = new ByteArray(result.getRow());
				map.put(b, kv);
				cache.cacheRow(cf, result.getRow(), kv.getValue());
			}
			for (byte[] key : keysToLookup) {
				ByteArray baKey = new ByteArray(key);
				if (!map.containsKey(baKey)) {
					KeyValue<Row> kv = new KeyValue<Row>();
					kv.setKey(key);
					kv.setValue(null);
					map.put(baKey, kv);
					cache.cacheRow(cf, key, kv.getValue());
				}
			}
		}
	}

	private void processColumns(List<org.apache.hadoop.hbase.KeyValue> hKeyValue, Row r) {
		for (org.apache.hadoop.hbase.KeyValue col : hKeyValue) {
			r.setKey(col.getRow());
			byte[] name = col.getQualifier();
			byte[] val = col.getValue();
			Column c = new Column();
			c.setName(name);
			if (val.length != 0)
				c.setValue(val);
			r.put(c);
		}
	}
}
