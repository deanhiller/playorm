package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession.CreateColumnSliceCallback;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

class CursorColumnSlice<T> extends AbstractCursor<T> {

	private static final Logger log = LoggerFactory.getLogger(CursorColumnSlice.class);

	private String columnName;
	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener batchListener;
	private Integer batchSize;

	private RowQuery<byte[], byte[]> query;
	private String info;
	private int pointer = -1;
	private List<Column<byte[]>> subList;
	private Boolean forward = null;
	
	public CursorColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener, Integer batchSize, String logInfo, String colName) {
		this.callback = l;
		this.isComposite = isComposite;
		this.batchListener = bListener;
		this.batchSize = batchSize;
		this.info = logInfo;
		this.columnName = colName;
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorColumnSlice["+tabs+info+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		//FOR some reason in astyanax, we have to keep getting the row query on resets or this Cursor returns 5 rows instead of the
		//SAME two rows it returned the first time.....well, form one of the unit tests
		 query = callback.createRowQuery();
		 pointer = -1;
		 subList = null;
		 forward = true;
	}
	
	@Override
	public void afterLast() {
		//FOR some reason in astyanax, we have to keep getting the row query on resets or this Cursor returns 5 rows instead of the
		//SAME two rows it returned the first time.....well, form one of the unit tests
		 query = callback.createRowQueryReverse();
		 pointer = -1;
		 subList = null;
		 forward = false;
	}

	@Override
	public Holder<T> nextImpl() {
		if(!forward)
			throw new IllegalStateException("You must call beforeFirst to traverse the cursor forward, you cannot call next after calling previous due to limitations of talking to noSql apis");
		try {
			fetchMoreResultsImpl();
			pointer++;
			if(pointer >= subList.size())
				return null; //no more results
			Column<byte[]> column = subList.get(pointer);
			
			return buildHolder(column);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Holder<T> previousImpl() {
		if(forward)
			throw new IllegalStateException("You must call afterLast to traverse reverse.  You cannot call previous after calling next due to limitations of calling into noSQL apis");
		try {
			fetchMoreResultsImpl();
			pointer++;
			if(pointer >= subList.size())
				return null; //no more results
			
			Column<byte[]> column = subList.get(pointer);
			return buildHolder(column);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Holder<T> buildHolder(com.netflix.astyanax.model.Column<byte[]> col) {

		if(isComposite) {
			IndexColumn c = convertToIndexCol(col, columnName);
			return new Holder<T>((T) c);
		} else {
			Object obj = col.getName();
			com.alvazan.orm.api.z8spi.action.Column c = new com.alvazan.orm.api.z8spi.action.Column();
			byte[] name = (byte[])obj;
			c.setName(name);
			c.setValue(col.getByteArrayValue());
			c.setTimestamp(col.getTimestamp());
			return new Holder<T>((T) c);
		}
	}

	public static IndexColumn convertToIndexCol(com.netflix.astyanax.model.Column<byte[]> col, String colNameStr) {
		Object colName = col.getName();
		GenericComposite bigDec = (GenericComposite)colName;
		IndexColumn c = new IndexColumn();
		c.setColumnName(colNameStr);
		c.setPrimaryKey(bigDec.getPk());
		c.setIndexedValue(bigDec.getIndexedValue());
		c.setValue(col.getByteArrayValue());
		return c;
	}
	
	private void fetchMoreResultsImpl() throws ConnectionException {
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
		long start = System.currentTimeMillis();
		OperationResult<ColumnList<byte[]>> opResult = query.execute();
		if (log.isDebugEnabled())
			log.debug("executing query "+query+" took "+(System.currentTimeMillis()-start));

		ColumnList<byte[]> columns = opResult.getResult();

		if(columns.isEmpty())
			subList = new ArrayList<Column<byte[]>>();
		else 
			fillInList(columns);
		
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(columns.size());
	}
	
	private void fillInList(ColumnList<byte[]> columns) {
		subList = new ArrayList<Column<byte[]>>();
		Iterator<Column<byte[]>> iter = columns.iterator();
		while(iter.hasNext()) {
			Column<byte[]> col = iter.next();
			subList.add(col);
		}
	}
}
