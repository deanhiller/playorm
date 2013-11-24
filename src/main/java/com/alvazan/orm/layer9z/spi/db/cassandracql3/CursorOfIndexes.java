package com.alvazan.orm.layer9z.spi.db.cassandracql3;

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
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class CursorOfIndexes extends AbstractCursor<IndexColumn> {

    private byte[] rowKey;
    private Integer batchSize;
    private BatchListener batchListener;
    private Session session;
    private String keySpace;
    private ListIterator<com.datastax.driver.core.Row> cachedRows;
    private String indTable;
    private boolean needToGetBatch;
    private Key from;
    private Key to;
    private DboColumnMeta columnMeta;

    public CursorOfIndexes(byte[] rowKeys, Integer batchSize, BatchListener list, String indTable, Key from, Key to) {
        this.rowKey = rowKeys;
        this.batchSize = batchSize;
        this.batchListener = list;
        this.indTable = indTable;
        this.from = from;
        this.to = to;
        this.needToGetBatch = true;
        this.cachedRows = null;
        beforeFirst();
    }

    @Override
    public String toString() {
        String tabs = StringLocal.getAndAdd();
        String keys = "" + rowKey;
        String retVal = "CursorOfIndexes[" + tabs + keys + tabs + "]";
        StringLocal.set(tabs.length());
        return retVal;
    }

    public void setupMore(String keyspace2, DboColumnMeta colMeta, Session session2) {
        if (keyspace2 == null)
            throw new IllegalArgumentException("DB was null");
        this.keySpace = keyspace2;
        this.columnMeta = colMeta;
        this.session = session2;
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
        IndexColumn indexCol = Cql3Util.convertToIndexCol(cachedRows.next(), indTable);
        return new Holder<IndexColumn>(indexCol);
    }

    @Override
    public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
        loadCache(true);
        if (cachedRows == null || !cachedRows.hasPrevious())
            return null;
        IndexColumn indexCol = Cql3Util.convertToIndexCol(cachedRows.previous(), indTable);
        return new Holder<IndexColumn>(indexCol);
    }

    private void loadCache(boolean reverse) {
        if (cachedRows != null && cachedRows.hasNext())
            return; // There are more rows so return and the code will return
                    // the next result from cache

        ResultSet resultSet = null;

        if (needToGetBatch) {
            if (batchListener != null)
                batchListener.beforeFetchingNextBatch();

            String rowKeyString = StandardConverters.convertFromBytes(String.class, rowKey);
            Select selectQuery = QueryBuilder.select().all().from(keySpace, indTable).allowFiltering();
            Where whereClause = Cql3Util.createRowQuery(from, to, columnMeta, selectQuery, rowKeyString, indTable);
            Query query = null;

            if (batchSize != null)
                query = whereClause.limit(batchSize);
            else
                query = whereClause.disableTracing();
            resultSet = session.execute(query);

            // Need to see where we use this batchListener
            if (batchListener != null && batchSize != null)
                batchListener.afterFetchingNextBatch(batchSize);

            List<com.datastax.driver.core.Row> finalRes = new ArrayList<com.datastax.driver.core.Row>();
            fillinCache(finalRes, resultSet);
            needToGetBatch = false;
            if (reverse) {
                while (cachedRows.hasNext())
                    cachedRows.next();
            }
        }
    }

    private void fillinCache(List<com.datastax.driver.core.Row> finalRes, ResultSet cursor) {
        if (cursor == null) {
            cachedRows = new ArrayList<com.datastax.driver.core.Row>().listIterator();
        } else {
            for (com.datastax.driver.core.Row row : cursor) {
                finalRes.add(row);
            }
            cachedRows = finalRes.listIterator();
        }
    }

}
