package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

class CursorColumnSliceMDB<T> extends AbstractCursor<T> {

	private DboTableMeta tableName;
	private BatchListener batchListener;
	private Integer batchSize;
	private DB db;
	private int pointer = -1;
	private List<DBObject> subList;
	private Boolean forward = null;
	private byte[] from;
	private byte[] to;
	private byte[] rowKey;
	
	public CursorColumnSliceMDB(DboTableMeta cfName, BatchListener bListener, Integer batchSize, DB keyspace, byte[] rowKey, byte[] from, byte[] to) {
		this.batchListener = bListener;
		this.batchSize = batchSize;
		this.tableName = cfName;
		this.db = keyspace;
		this.from = from;
		this.to = to;
		this.rowKey = rowKey;
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

		DBCollection dbCollection = null;
		if (db != null && db.collectionExists(tableName.getColumnFamily())) {
			dbCollection = db.getCollection(tableName.getColumnFamily());
		} else
			return;

		DBObject row = dbCollection.findOne(rowKey);
		if(row == null) {
			return;
		}
		else {
			Set<String> fieldSet = row.keySet();
			//String fromField = StandardConverters.convertToString(from);
			//String toField = StandardConverters.convertToString(to);
			//String fromField = StandardConverters.convertFromBytes(String.class, from);
			//String toField = StandardConverters.convertFromBytes(String.class, to);
			ByteArray fromField = new ByteArray(from);
			ByteArray toField = new ByteArray(to);
			
/*			for (String field : fieldSet) {				
				if(!field.equalsIgnoreCase("_id")) {
					byte[] name = StandardConverters.convertFromString(byte[].class, field);
					ByteArray fieldname = new ByteArray(name);
					if (fieldname.compareTo(fromField) >= 0) {
						if(fieldname.compareTo(toField) <= 0) {
							Object value = row.get(field);
							subList.add(new BasicDBObject(field, value));
						}
					}
				}
			}*/
		}
		if (subList == null) {
			subList = new ArrayList<DBObject>();
		}
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(subList.size());
	}
	
}
