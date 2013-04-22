package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.BatchListener;
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

public class CursorForValues extends AbstractCursor<IndexColumn> {

	private byte[] rowKey;
	private BatchListener batchListener;
	private DB db;
	private ListIterator<IndexColumn> cachedRows;
	private String indTable;
	private boolean needToGetBatch;
	private DboColumnMeta columnMeta;
	private List<byte[]> values;

	public CursorForValues(byte[] rowKeys, BatchListener list, String indTable,
			List<byte[]> keys) {
		this.rowKey = rowKeys;
		this.batchListener = list;
		this.indTable = indTable;
		this.needToGetBatch = true;
		this.cachedRows = null;
		this.values = keys;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKey;
		String retVal = "CursorForValues(MongoDB)[" + tabs + keys
				+ tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	public void setupMore(DB keyspace, DboColumnMeta colMeta) {
		if (keyspace == null)
			throw new IllegalArgumentException("DB was null");
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
		loadCache(false);
		if (cachedRows == null || !cachedRows.hasNext())
			return null;

		return new Holder<IndexColumn>(cachedRows.next());
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		loadCache(true);
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;

		return new Holder<IndexColumn>(cachedRows.previous());
	}

	private void loadCache(boolean reverse) {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache

		DBCursor cursor = null;
		DBCollection dbCollection = null;

		if (db != null && db.collectionExists(this.indTable)) {
			dbCollection = db.getCollection(this.indTable);
		} else
			return;

		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();

			BasicDBObject query = new BasicDBObject();
			BasicDBObject rangeQuery = MongoDbUtil.createRowQueryFromValues(
					values, columnMeta);
			query.put("i",
					StandardConverters.convertFromBytes(String.class, rowKey));
			if (!rangeQuery.isEmpty())
				query.append("k", rangeQuery);
			BasicDBObject fields = new BasicDBObject();
			fields.put("_id", -1);
			fields.append("k", 1);
			fields.append("v", 1);

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
			needToGetBatch = false;
			if(reverse) {
				while (cachedRows.hasNext())
					cachedRows.next();
			}
		}

	}

}
