package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ScanInfo;
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

public class CursorForValues extends AbstractCursor<IndexColumn> {

    private byte[] rowKey;
    private BatchListener batchListener;
    private Session session;
    private ListIterator<IndexColumn> cachedRows;
    private String indTable;
    private String keySpace;
    private boolean needToGetBatch;
    private DboColumnMeta columnMeta;
    private List<byte[]> values;

    public CursorForValues(ScanInfo scanInfo, BatchListener list, List<byte[]> keys, Session session2, String keySpace2) {
        this.rowKey = scanInfo.getRowKey();
        this.batchListener = list;
        this.indTable = scanInfo.getIndexColFamily();
        this.cachedRows = null;
        this.values = keys;
        this.session = session2;
        this.keySpace = keySpace2;
        this.columnMeta = scanInfo.getColumnName();
        this.needToGetBatch = true;
        beforeFirst();

    }

    @Override
    public String toString() {
        String tabs = StringLocal.getAndAdd();
        String keys = "" + rowKey;
        String retVal = "CursorForValues(CQL3)[" + tabs + keys + tabs + "]";
        StringLocal.set(tabs.length());
        return retVal;
    }

    public void setupMore(String keySpace2, DboColumnMeta colMeta, Session session2) {
        if (keySpace2 == null)
            throw new IllegalArgumentException("DB was null");

        this.columnMeta = colMeta;

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

        ResultSet resultSet = null;

        if (needToGetBatch) {
            if (batchListener != null)
                batchListener.beforeFetchingNextBatch();

            String rowKeyString = StandardConverters.convertFromBytes(String.class, rowKey);

            Select selectQuery = QueryBuilder.select().all().from(keySpace, indTable).allowFiltering();
            Where whereClause = Cql3Util.createRowQueryFromValues(values, columnMeta, selectQuery, rowKeyString);
            Query query = whereClause.disableTracing();

            // System.out.println("QUERY IS: " + query);
            resultSet = session.execute(query);

            if (batchListener != null)
                batchListener.afterFetchingNextBatch(10);

            List<IndexColumn> finalRes = new ArrayList<IndexColumn>();

            if (resultSet == null) {
                cachedRows = new ArrayList<IndexColumn>().listIterator();
            } else {
                for (com.datastax.driver.core.Row row : resultSet) {
                    IndexColumn indexCol = Cql3Util.convertToIndexCol(row, indTable);
                    finalRes.add(indexCol);
                }
                cachedRows = finalRes.listIterator();
            }
            cachedRows = finalRes.listIterator();
            needToGetBatch = false;
            if (reverse) {
                while (cachedRows.hasNext())
                    cachedRows.next();
            }
        }

    }

}
