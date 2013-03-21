package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.inject.Provider;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class CursorKeysToRowsMDB extends AbstractCursor<KeyValue<Row>> {

	private Info info;
	private DirectCursor<byte[]> rowKeys;
	private int batchSize;
	private BatchListener list;
	private DB db;
	private ListIterator<KeyValue<Row>> cachedRows;
	private Provider<Row> rowProvider;
	private Cache cache;
	private DboTableMeta cf;

	public CursorKeysToRowsMDB(DirectCursor<byte[]> rowKeys, int batchSize,
			BatchListener list, Provider<Row> rowProvider,
			DboTableMeta colFamily) {
		this.rowProvider = rowProvider;
		this.rowKeys = rowKeys;
		this.batchSize = batchSize;
		this.list = list;
		this.cf = colFamily;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKeys;
		if (rowKeys instanceof List)
			keys = "List" + keys;
		String retVal = "CursorKeysToRowsMDB(MongoDBFindRows)[" + tabs + keys
				+ tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	public void setupMore(DB keyspace, DboTableMeta cf, Info info, Cache cache) {
		if (cache == null || keyspace == null || cf == null | info == null)
			throw new IllegalArgumentException(
					"no params can be null but one was null");
		this.cf = cf;
		this.info = info;
		this.cache = cache;
		this.db = keyspace;
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

	@SuppressWarnings("unchecked")
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
		DBCursor cursor = null;
		// / NEED TO CHANGE THIS CODE LIKE CASSANDRA FOR ENABLE CACHING

		// DBCollection dbCollection = info.getColumnFamilyObj();
		DBCollection dbCollection = null;
		if (db != null && db.collectionExists(cf.getColumnFamily())) {
			dbCollection = db.getCollection(cf.getColumnFamily());
		} else
			return;

		if (keysToLookup.size() > 0) {
			if (list != null)
				list.beforeFetchingNextBatch();

			BasicDBObject query = new BasicDBObject();
			query.put("_id", new BasicDBObject("$in", keysToLookup));
			cursor = dbCollection.find(query);

			if (list != null)
				list.afterFetchingNextBatch(cursor.count());
		} else {
			cursor = new DBCursor(dbCollection, null, null, null);
		}

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while (cursor.hasNext()) {
			DBObject mdbrow = cursor.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			byte[] mdbRowKey = StandardConverters.convertToBytes(mdbrow
					.get("_id"));
			kv.setKey(mdbRowKey);

			if (!mdbrow.keySet().isEmpty()) {
				// Astyanax returns a row when there is none BUT we know if
				// there are 0 columns there is really no row in the database
				// then
				Row r = rowProvider.get();
				r.setKey(mdbRowKey);
				MongoDbUtil.processColumns(mdbrow, r);
				kv.setValue(r);
			}

			ByteArray b = new ByteArray(mdbRowKey);
			map.put(b, kv);
			cache.cacheRow(cf, mdbRowKey, kv.getValue());
		}

		// UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we
		// provided so, we need to iterate over the whole thing here
		// into our own List :( :( .

		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
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

	@SuppressWarnings("unchecked")
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

		DBCursor cursor = null;
		if (keysToLookup.size() > 0) {
			if (list != null)
				list.beforeFetchingNextBatch();
			/*
			 * ColumnFamily<byte[], byte[]> cf = info.getColumnFamilyObj();
			 * ColumnFamilyQuery<byte[], byte[]> q2 = keyspace.prepareQuery(cf);
			 * RowSliceQuery<byte[], byte[]> slice =
			 * q2.getKeySlice(keysToLookup);
			 * 
			 * OperationResult<Rows<byte[], byte[]>> result = execute(slice);
			 * 
			 * Rows<byte[], byte[]> rows = result.getResult(); resultingRows =
			 * rows.iterator(); if(list != null)
			 * list.afterFetchingNextBatch(rows.size());
			 */
		} else {
			cursor = new DBCursor(null, null, null, null);
		}

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while (cursor.hasNext()) {
			DBObject mongoDoc = cursor.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			/*
			 * kv.setKey(row.getKey()); if(!mongoDoc.getColumns().isEmpty()) {
			 * //Astyanax returns a row when there is none BUT we know if there
			 * are 0 columns there is really no row in the database //then Row r
			 * = rowProvider.get(); r.setKey(row.getKey());
			 * //MongoDbUtil.processColumns(row, r); kv.setValue(r); }
			 * 
			 * ByteArray b = new ByteArray(row.getKey()); map.put(b, kv);
			 * cache.cacheRow(cf, row.getKey(), kv.getValue());
			 */
		}

		// UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we
		// provided so, we need to iterate over the whole thing here
		// into our own List :( :( .

		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
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
		while (cachedRows.hasNext())
			cachedRows.next();
	}

}
