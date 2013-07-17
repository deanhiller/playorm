package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class ScanMongoDbCollection extends AbstractCursor<IndexColumn> {

    private Integer batchSize;
    private BatchListener batchListener;
    private DB db;
    private ListIterator<DBObject> cachedRows;
    private String indTable;
    private boolean needToGetBatch;

    public ScanMongoDbCollection(Integer batchSize, BatchListener list, String dbCollection, DB keyspace) {
        this.batchSize = batchSize;
        this.batchListener = list;
        this.indTable = dbCollection;
        this.needToGetBatch = true;
        this.cachedRows = null;
        this.db = keyspace;
        beforeFirst();
    }

    @Override
    public String toString() {
        String tabs = StringLocal.getAndAdd();
        String retVal = "ScanMongoDbCollection[" + tabs + "]";
        StringLocal.set(tabs.length());
        return retVal;
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
        IndexColumn indexCol = convertToIndexColFromId(cachedRows.next());
        return new Holder<IndexColumn>(indexCol);
    }

    @Override
    public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
        loadCache(true);
        if (cachedRows == null || !cachedRows.hasPrevious())
            return null;
        IndexColumn indexCol = convertToIndexColFromId(cachedRows.previous());
        return new Holder<IndexColumn>(indexCol);
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
            if (batchSize != null)
                cursor = dbCollection.find().batchSize(batchSize);
            else
                cursor = dbCollection.find();

            if (batchListener != null)
                batchListener.afterFetchingNextBatch(cursor.count());

            List<DBObject> finalRes = new ArrayList<DBObject>();
            fillinCache(finalRes, cursor);
            needToGetBatch = false;
            if (reverse) {
                while (cachedRows.hasNext())
                    cachedRows.next();
            }
        }
    }

    private void fillinCache(List<DBObject> finalRes, DBCursor cursor) {
        if (cursor.size() == 0) {
            cachedRows = new ArrayList<DBObject>().listIterator();
        } else {
            while (cursor.hasNext()) {
                DBObject mdbrow = cursor.next();
                finalRes.add(mdbrow);
            }
            cachedRows = finalRes.listIterator();
        }
    }

    private IndexColumn convertToIndexColFromId(DBObject col) {
        Object pk = col.get("_id");
        IndexColumn c = new IndexColumn();
        // c.setColumnName(columnName); Will we ever need this now?
        if (pk != null) {
            c.setPrimaryKey((byte[]) pk);
            c.setIndexedValue((byte[]) pk);
        }
        c.setValue(null);
        return c;
    }
}
