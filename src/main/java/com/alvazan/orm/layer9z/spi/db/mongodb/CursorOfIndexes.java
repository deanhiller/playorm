package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class CursorOfIndexes extends AbstractCursor<IndexColumn> {

	private byte[] rowKey;
	private Integer batchSize;
	private BatchListener batchListener;
	private DB db;
	private ListIterator<IndexColumn> cachedRows;
	//private Cache cache;
	private String indTable;
	private boolean needToGetBatch;
	private Key from;
	private Key to;
	private DboColumnMeta columnMeta;

	public CursorOfIndexes(byte[] rowKeys, Integer batchSize,
			BatchListener list,	String indTable, Key from, Key to) {
		this.rowKey = rowKeys;
		this.batchSize = batchSize;
		this.batchListener = list;
		this.indTable = indTable;
		this.from = from;
		this.to = to;
		this.needToGetBatch = true;
		this.cachedRows = null;
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

	public void setupMore(DB keyspace, DboColumnMeta colMeta) {
		if (keyspace == null)
			throw new IllegalArgumentException(
					"DB was null");
		this.db = keyspace;
		this.columnMeta = colMeta;
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
			BasicDBObject rangeQuery = MongoDbUtil.createRowQuery(from, to, columnMeta);
			query.put("i", StandardConverters.convertFromBytes(String.class, rowKey));
			if(!rangeQuery.isEmpty())
				query.append("k", rangeQuery);
			BasicDBObject fields = new BasicDBObject();
			fields.put("_id", -1);
			fields.append("k", 1);
			fields.append("v", 1);
			if (batchSize != null)
				cursor = dbCollection.find(query, fields).batchSize(batchSize);
			else
				cursor = dbCollection.find(query, fields);

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

	private void loadCacheBackward() {
		if (cachedRows != null && cachedRows.hasPrevious())
			return; // There are more rows so return and the code will return
					// the next result from cache

		DBCursor cursor = null;
		// / NEED TO CHANGE THIS CODE LIKE CASSANDRA to ENABLE CACHING

		DBCollection dbCollection = null;
		if (db != null && db.collectionExists(this.indTable)) {
			dbCollection = db.getCollection(this.indTable);
		} else
			return;

		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();
			BasicDBObject query = new BasicDBObject();
			query.put("i", StandardConverters.convertFromBytes(String.class, rowKey));
			BasicDBObject rangeQuery = MongoDbUtil.createRowQuery(from, to, columnMeta);
			if(!rangeQuery.isEmpty())
				query.append("k", rangeQuery);
			BasicDBObject fields = new BasicDBObject();
			fields.put("_id", -1);
			fields.append("k", 1);
			fields.append("v", 1);
			if (batchSize != null)
				cursor = dbCollection.find(query, fields).batchSize(batchSize);
			else
				cursor = dbCollection.find(query, fields);

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
/*		while (cachedRows.hasNext())
			cachedRows.next();*/
	}

}
