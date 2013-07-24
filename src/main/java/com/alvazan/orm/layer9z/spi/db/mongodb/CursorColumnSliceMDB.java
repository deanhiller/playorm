package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


class CursorColumnSliceMDB<T> extends AbstractCursor<T> {

	private DBCollection dbCollection;
	private BatchListener batchListener;
	private Integer batchSize;
	private int pointer = -1;
	private List<DBObject> subList;
	private Boolean forward = null;
	private byte[] from;
	private byte[] to;
	private byte[] rowKey;
	private Class columnNameType;
	
	public CursorColumnSliceMDB(ColumnSliceInfo sliceInfo, BatchListener bListener, Integer batchSize, DBCollection dbCollection2) {
		this.batchListener = bListener;
		this.batchSize = null;
		dbCollection = dbCollection2;
		this.from = sliceInfo.getFrom();
		this.to = sliceInfo.getTo();
		this.rowKey = sliceInfo.getRowKey();
        this.columnNameType = sliceInfo.getColumnNameType();
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorColumnSlice["+tabs+tabs+"]";
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
	public Holder<T> nextImpl() {
		if(!forward)
			throw new IllegalStateException("You must call beforeFirst to traverse the cursor forward, you cannot call next after calling previous due to limitations of talking to noSql apis");
		fetchMoreResultsImpl();
		pointer++;
		if (pointer >= subList.size())
			return null; // no more results
		DBObject column = subList.get(pointer);

		return buildHolder(column);
	}
	
	@Override
	public Holder<T> previousImpl() {
		if(forward)
			throw new IllegalStateException("You must call afterLast to traverse reverse.  You cannot call previous after calling next due to limitations of calling into noSQL apis");
		fetchMoreResultsImpl();
		pointer++;
		if (pointer >= subList.size())
			return null; // no more results

		DBObject column = subList.get(pointer);
		return buildHolder(column);
	}

	@SuppressWarnings("unchecked")
	private Holder<T> buildHolder(DBObject col) {
		Set<String> singleKey = col.keySet();
		String colName = singleKey.iterator().next();
		byte[] name = StandardConverters.convertFromString(byte[].class, colName);
		byte[] val = StandardConverters.convertToBytes(col.get(colName));
		Column c = new Column();
		c.setName(name);
		if (val.length != 0)
			c.setValue(val);
		return new Holder<T>((T) c);

	}

	private void fetchMoreResultsImpl() {
		if(subList != null){
			//If subIterator is not null, we have already previously fetched results!!!
			if(pointer < subList.size()-1)
				return; //no need to fetch next subiterator since this subIterator has more
			else if(batchSize == null) //then we already fetched everything in first round
				return;
			else if(subList.size() < batchSize) {
				//since we have previous results, we then have a count of those results BUT they
				//did NOT fill up the batch so no need to go to database as we know it has no more results
				return;
			}
		}

		//reset the point...
		pointer = -1;
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();

		DBObject row = dbCollection.findOne(rowKey);
		if(row == null) {
			return;
		}
		else {
            Set<String> fieldSet = row.keySet();
            if (BigInteger.class.equals(columnNameType)) {
                intColumnSlice(fieldSet, row);
            } else if (BigDecimal.class.equals(columnNameType)) {
                decimalColumnSlice(fieldSet, row);
            } else if (String.class.equals(columnNameType)) {
                stringColumSlice(fieldSet, row);
            } else 
                throw new UnsupportedOperationException("Type " + columnNameType.getName() + " is not allowed for ColumnSlice");
        }
		if (subList == null) {
			subList = new ArrayList<DBObject>();
		}
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(subList.size());
	}

    private void intColumnSlice(Set<String> fieldSet, DBObject row) {
        subList = new ArrayList<DBObject>();
        Map<BigInteger, Object> map = new TreeMap<BigInteger, Object>();
        BigInteger fromField = StandardConverters.convertFromBytes(BigInteger.class, from);
        BigInteger toField = StandardConverters.convertFromBytes(BigInteger.class, to);
        for (String field : fieldSet) {
            if (!field.equalsIgnoreCase("_id")) {
                BigInteger name = StandardConverters.convertFromBytes(BigInteger.class, StandardConverters.convertFromString(byte[].class, field));
                if (name.compareTo(fromField) >= 0 && name.compareTo(toField) <= 0) {
                    Object value = row.get(field);
                    map.put(name, value);
                }
            }
        }
        for (BigInteger field : map.keySet()) {
            String newField = StandardConverters.convertToString(StandardConverters.convertToBytes(field));
            BasicDBObject dbObject = new BasicDBObject(newField, map.get(field));
            subList.add(dbObject);
        }
    }

    private void decimalColumnSlice(Set<String> fieldSet, DBObject row) {
        subList = new ArrayList<DBObject>();
        Map<BigDecimal, Object> map = new TreeMap<BigDecimal, Object>();
        BigDecimal fromField = StandardConverters.convertFromBytes(BigDecimal.class, from);
        BigDecimal toField = StandardConverters.convertFromBytes(BigDecimal.class, to);
        for (String field : fieldSet) {
            if (!field.equalsIgnoreCase("_id")) {
                BigDecimal name = StandardConverters.convertFromBytes(BigDecimal.class, StandardConverters.convertFromString(byte[].class, field));
                if (name.compareTo(fromField) >= 0 && name.compareTo(toField) <= 0) {
                    Object value = row.get(field);
                    map.put(name, value);
                }
            }
        }
        for (BigDecimal field : map.keySet()) {
            String newField = StandardConverters.convertToString(StandardConverters.convertToBytes(field));
            BasicDBObject dbObject = new BasicDBObject(newField, map.get(field));
            subList.add(dbObject);
        }
    }

    private void stringColumSlice(Set<String> fieldSet, DBObject row) {
        subList = new ArrayList<DBObject>();
        Map<String, Object> map = new TreeMap<String, Object>();
        String fromField = StandardConverters.convertFromBytes(String.class, from);
        String toField = StandardConverters.convertFromBytes(String.class, to);
        for (String field : fieldSet) {
            if (!field.equalsIgnoreCase("_id")) {
                if (field.compareTo(fromField) >= 0 && field.compareTo(toField) <= 0) {
                    Object value = row.get(field);
                    map.put(field, value);
                }
            }
        }
        for (String field : map.keySet()) {
            BasicDBObject dbObject = new BasicDBObject(field, map.get(field));
            subList.add(dbObject);
        }
    }
}
