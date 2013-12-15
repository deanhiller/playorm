package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

class CursorColumnSliceCql<T> extends AbstractCursor<T> {

    private BatchListener batchListener;
    private Integer batchSize;
    private String keys;
    private int pointer = -1;
    private ColumnSliceInfo sliceInfo;
    private List<com.datastax.driver.core.Row> subList;
    private byte[] from;
    private byte[] to;
    private byte[] rowKey;
    private Boolean forward = null;
    private String table;
    private Class columnNameType = null;
    private Session session = null;

    public CursorColumnSliceCql(ColumnSliceInfo sliceInfo2, BatchListener bListener, Integer batchSize, String keys2, String table2, Session session2) {
        this.batchListener = bListener;
        this.batchSize = batchSize;
        this.keys = keys2;
        this.from = sliceInfo2.getFrom();
        this.to = sliceInfo2.getTo();
        this.table = table2;
        this.rowKey = sliceInfo2.getRowKey();
        this.sliceInfo = sliceInfo2;
        this.columnNameType = sliceInfo2.getColumnNameType();
        this.session = session2;
        beforeFirst();

    }

    @Override
    public String toString() {
        String tabs = StringLocal.getAndAdd();
        String retVal = "CursorColumnSliceCql[" + tabs + tabs + "]";
        StringLocal.set(tabs.length());
        return retVal;
    }

    @Override
    public void beforeFirst() {
        pointer = -1;
        subList = null;
        forward = true;

    }

    @Override
    public void afterLast() {
        pointer = -1;
        subList = null;
        forward = false;
    }

    @Override
    public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> nextImpl() {
        if (!forward)
            throw new IllegalStateException(
                    "You must call beforeFirst to traverse the cursor forward, you cannot call next after calling previous due to limitations of talking to noSql apis");
        fetchMoreResultsImpl();
        pointer++;
        if (pointer >= subList.size())
            return null; // no more results
        com.datastax.driver.core.Row column = subList.get(pointer);
        return buildHolder(column);

    }

    @Override
    public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> previousImpl() {
        if (forward)
            throw new IllegalStateException(
                    "You must call afterLast to traverse reverse.  You cannot call previous after calling next due to limitations of calling into noSQL apis");
        fetchMoreResultsImpl();
        pointer++;
        if (pointer >= subList.size())
            return null; // no more results
        com.datastax.driver.core.Row column = subList.get(pointer);
        return buildHolder(column);
    }

    private void fetchMoreResultsImpl() {
        if (subList != null) {
            if (pointer < subList.size()) {
                return;
            } else if (batchSize == null) {
                return;
            } else if (subList.size() < batchSize) {
                return;
            }
        }

        pointer = -1;

        if (batchListener != null)
            batchListener.beforeFetchingNextBatch();
        if (subList != null)
            return;

        if (batchListener != null)
            batchListener.beforeFetchingNextBatch();

        columnNameType = sliceInfo.getColumnNameType();
        ResultSet resultSet = null;
        Clause rkClause = QueryBuilder.eq("id", ByteBuffer.wrap(rowKey));
        Query query = null;
        query = QueryBuilder.select().all().from(keys, table).where(rkClause).disableTracing();

        subList = new ArrayList<com.datastax.driver.core.Row>();
        try {
            resultSet = session.execute(query);
            if (resultSet == null) {
                return;
            } else {
                if (BigInteger.class.equals(columnNameType)) {
                    intColumnSlice(resultSet);
                } else if (BigDecimal.class.equals(columnNameType)) {
                    decimalColumnSlice(resultSet);
                } else if (String.class.equals(columnNameType)) {
                    stringColumSlice(resultSet);
                } else
                    throw new UnsupportedOperationException("Type " + columnNameType.getName() + " is not allowed for ColumnSlice");

            }
            if (batchListener != null)
                batchListener.afterFetchingNextBatch(2);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> buildHolder(com.datastax.driver.core.Row column) {
        byte[] name = StandardConverters.convertFromString(byte[].class, column.getString("colname"));
        ByteBuffer data = column.getBytes("colvalue");
        byte[] val = new byte[data.remaining()];
        data.get(val);
        Column c = new Column();
        c.setName(name);
        if (val.length != 0)
            c.setValue(val);
        return new Holder<T>((T) c);
    }

    private void intColumnSlice(ResultSet resultSet) {
        Map<BigInteger, com.datastax.driver.core.Row> map = new TreeMap<BigInteger, com.datastax.driver.core.Row>();
        BigInteger fromField = StandardConverters.convertFromBytes(BigInteger.class, from);
        BigInteger toField = StandardConverters.convertFromBytes(BigInteger.class, to);
        for (com.datastax.driver.core.Row row : resultSet) {
            String field = row.getString("colname");
            BigInteger name = StandardConverters.convertFromBytes(BigInteger.class, StandardConverters.convertFromString(byte[].class, field));
            if (name.compareTo(fromField) >= 0 && name.compareTo(toField) <= 0) {
                map.put(name, row);
            }
        }
        for (BigInteger field : map.keySet()) {
            com.datastax.driver.core.Row row = map.get(field);
            subList.add(row);
        }
    }

    private void decimalColumnSlice(ResultSet resultSet) {
        Map<BigDecimal, com.datastax.driver.core.Row> map = new TreeMap<BigDecimal, com.datastax.driver.core.Row>();
        BigDecimal fromField = StandardConverters.convertFromBytes(BigDecimal.class, from);
        BigDecimal toField = StandardConverters.convertFromBytes(BigDecimal.class, to);
        for (com.datastax.driver.core.Row row : resultSet) {
            String field = row.getString("colname");
            BigDecimal name = StandardConverters.convertFromBytes(BigDecimal.class, StandardConverters.convertFromString(byte[].class, field));
            if (name.compareTo(fromField) >= 0 && name.compareTo(toField) <= 0) {
                map.put(name, row);
            }
        }
        for (BigDecimal field : map.keySet()) {
            com.datastax.driver.core.Row row = map.get(field);
            subList.add(row);
        }
    }

    private void stringColumSlice(ResultSet resultSet) {
        Map<String, com.datastax.driver.core.Row> map = new TreeMap<String, com.datastax.driver.core.Row>();
        String fromField = StandardConverters.convertFromBytes(String.class, from);
        String toField = StandardConverters.convertFromBytes(String.class, to);
        for (com.datastax.driver.core.Row row : resultSet) {
            String field = row.getString("colname");
            if (field.compareTo(fromField) >= 0 && field.compareTo(toField) <= 0) {
                map.put(field, row);
            }
        }
        for (String field : map.keySet()) {
            com.datastax.driver.core.Row row = map.get(field);
            subList.add(row);
        }
    }

}
