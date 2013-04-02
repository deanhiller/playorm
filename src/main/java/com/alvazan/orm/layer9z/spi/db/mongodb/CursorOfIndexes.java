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
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class CursorOfIndexes extends AbstractCursor<IndexColumn> {

	private Info info;
	private byte[] rowKey;
	private int batchSize;
	private BatchListener batchListener;
	private DB db;
	private ListIterator<IndexColumn> cachedRows;
	private Provider<Row> rowProvider;
	private Cache cache;
	private String indTable;
	private boolean needToGetBatch;
	private Key from;
	private Key to;

	public CursorOfIndexes(byte[] rowKeys, int batchSize,
			BatchListener list, Provider<Row> rowProvider,
			String indTable, Key from, Key to) {
		this.rowProvider = rowProvider;
		this.rowKey = rowKeys;
		this.batchSize = batchSize;
		this.batchListener = list;
		this.indTable = indTable;
		this.from = from;
		this.to = to;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKey;
		String retVal = "CursorKeysToRowsMDB(MongoDBFindRows)[" + tabs + keys
				+ tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	public void setupMore(DB keyspace) {
		if (keyspace == null)
			throw new IllegalArgumentException(
					"DB was null");
		this.db = keyspace;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		cachedRows = null;
		needToGetBatch = true;
	}

	@Override
	public void afterLast() {
		cachedRows = null;
		needToGetBatch = true;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> nextImpl() {
		loadCache();
		if (cachedRows == null || !cachedRows.hasNext())
			return null;

		return new Holder<IndexColumn>(cachedRows.next());
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		loadCacheBackward();
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;

		return new Holder<IndexColumn>(cachedRows.previous());
	}

	@SuppressWarnings("unchecked")
	private void loadCache() {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache

		DBCursor cursor = null;
		// / NEED TO CHANGE THIS CODE LIKE CASSANDRA to ENABLE CACHING

		// DBCollection dbCollection = info.getColumnFamilyObj();
		DBCollection dbCollection = null;
		if (db != null && db.collectionExists(this.indTable)) {
			dbCollection = db.getCollection(this.indTable);
		} else
			return;

		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();

			BasicDBObject query = new BasicDBObject();
			//query.put("_id", new BasicDBObject("$in", keysToLookup));
			//System.out.println("rowKey " + rowKey);
			//System.out.println("from.getKey() " + from.getKey());
			query.put("i", StandardConverters.convertFromBytes(String.class, rowKey));
			query.append("k", from.getKey());
			BasicDBObject fields = new BasicDBObject();
			fields.put("_id", -1);
			fields.append("k", 1);
			fields.append("v", 1);
			cursor = dbCollection.find(query, fields).batchSize(batchSize);

			if (batchListener != null)
				batchListener.afterFetchingNextBatch(cursor.count());
			
			List<IndexColumn> finalRes = new ArrayList<IndexColumn>();
			while (cursor.hasNext()) {
				DBObject mdbrow = cursor.next();
				IndexColumn indexCol = MongoDbUtil.convertToIndexCol(mdbrow);
				finalRes.add(indexCol);
			}
			cachedRows = finalRes.listIterator();
		} else {
			cursor = new DBCursor(dbCollection, null, null, null);
		}
		needToGetBatch = false;
	}

	@SuppressWarnings("unchecked")
	private void loadCacheBackward() {
		if (cachedRows != null && cachedRows.hasPrevious())
			return; // There are more rows so return and the code will return
					// the next result from cache

		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		DBCursor cursor = null;
		if (keysToLookup.size() > 0) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();

		} else {
			cursor = new DBCursor(null, null, null, null);
		}

		Map<ByteArray, IndexColumn> map = new HashMap<ByteArray, IndexColumn>();
		while (cursor.hasNext()) {
			DBObject mongoDoc = cursor.next();
			IndexColumn kv = new IndexColumn();
		}

		List<IndexColumn> finalRes = new ArrayList<IndexColumn>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		cachedRows = finalRes.listIterator();
		while (cachedRows.hasNext())
			cachedRows.next();
	}

}
