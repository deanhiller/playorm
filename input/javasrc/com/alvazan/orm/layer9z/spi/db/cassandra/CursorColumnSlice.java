package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.BatchListener;
//import com.alvazan.orm.api.z8spi.action.Column;
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

	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener batchListener;
	private Integer batchSize;

	private RowQuery<byte[], byte[]> query;
	private ListIterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
	private int count;
	private String info;
	
	public CursorColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener, Integer batchSize, String logInfo) {
		this.callback = l;
		this.isComposite = isComposite;
		this.batchListener = bListener;
		this.batchSize = batchSize;
		this.info = logInfo;
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
		 subIterator = null;
	}
	
	@Override
	public void afterLast() {
		//FOR some reason in astyanax, we have to keep getting the row query on resets or this Cursor returns 5 rows instead of the
		//SAME two rows it returned the first time.....well, form one of the unit tests
		 query = callback.createRowQueryReverse();
		 subIterator = null;
	}

	@Override
	public Holder<T> nextImpl() {
		try {
			ListIterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator = fetchMoreResultsImpl();
			if(latestIterator == null)
				return null;
			return nextColumn(latestIterator);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Holder<T> previousImpl() {
		try {
			ListIterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator = fetchMorePreviousResultsImpl();
			if(latestIterator == null)
				return null;
			return previousColumn(latestIterator);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Holder<T> nextColumn(ListIterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator) {
		com.netflix.astyanax.model.Column<byte[]> col = latestIterator.next();
		count++;
		
		return buildHolder(col);
	}
	
	@SuppressWarnings("unchecked")
	private Holder<T> previousColumn(ListIterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator) {
		com.netflix.astyanax.model.Column<byte[]> col = latestIterator.previous();
		count--;
		return buildHolder(col);
	}
	

	@SuppressWarnings("unchecked")
	private Holder<T> buildHolder(com.netflix.astyanax.model.Column<byte[]> col) {

		if(isComposite) {
			IndexColumn c = convertToIndexCol(col);
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

	public static IndexColumn convertToIndexCol(com.netflix.astyanax.model.Column<byte[]> col) {
		Object colName = col.getName();
		GenericComposite bigDec = (GenericComposite)colName;
		IndexColumn c = new IndexColumn();
		c.setPrimaryKey(bigDec.getPk());
		c.setIndexedValue(bigDec.getIndexedValue());
		c.setValue(col.getByteArrayValue());
		return c;
	}
	
	
	private ListIterator<com.netflix.astyanax.model.Column<byte[]>> fetchMoreResultsImpl() throws ConnectionException {
		if(subIterator != null){
			//If subIterator is not null, we have already previously fetched results!!!
			if(subIterator.hasNext())
				return subIterator; //no need to fetch next subiterator since this subIterator has more
			else if(batchSize == null) //then we already fetched everything in first round
				return null;
			else if(count < batchSize) {
				//since we have previous results, we then have a count of those results BUT they
				//did NOT fill up the batch so no need to go to database as we know it has no more results
				return null;
			}
		}
		
		count = 0;
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		OperationResult<ColumnList<byte[]>> opResult = query.execute();
		ColumnList<byte[]> columns = opResult.getResult();

		if(columns.isEmpty())
			subIterator = null; 
		else 
			subIterator = new OurColumnListIterator(columns);
		
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(columns.size());
		return subIterator;
	}
	
	private ListIterator<com.netflix.astyanax.model.Column<byte[]>> fetchMorePreviousResultsImpl() throws ConnectionException {
		if(subIterator != null){
			//If subIterator is not null, we have already previously fetched results!!!
			if(subIterator.hasPrevious())
				return subIterator; //no need to fetch previous subiterator since this subIterator has more
			else if(batchSize == null) //then we already fetched everything in first round
				return null;
			else if(Math.abs(count) < batchSize) {
				//since we have previous results, we then have a count of those results BUT they
				//did NOT fill up the batch so no need to go to database as we know it has no more results
				return null;
			}
		}
		
		count = -1;
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		OperationResult<ColumnList<byte[]>> opResult = query.execute();
		ColumnList<byte[]> columns = opResult.getResult();

		if(columns.isEmpty())
			subIterator = null; 
		else 
			subIterator = new OurColumnListIterator(columns);
		
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(columns.size());
		return subIterator;
	}
	
	private class OurColumnListIterator implements ListIterator<Column<byte[]>> {

		ColumnList<byte[]> columns = null;
		int currIndex=0;  //zero means *before* the zeroith element
		
		public OurColumnListIterator(ColumnList<byte[]> cols) {
			columns = cols;
		}
		
		@Override
		public void add(Column<byte[]> arg0) {
			throw new UnsupportedOperationException("We don't support this");
			
		}

		@Override
		public boolean hasNext() {
			return currIndex!=columns.size();
		}

		@Override
		public boolean hasPrevious() {
			return currIndex!=0;
		}

		@Override
		public Column<byte[]> next() {
			if(currIndex==columns.size())
				return null;
			return columns.getColumnByIndex(currIndex++);
		}

		@Override
		public int nextIndex() {
			return currIndex;
		}

		@Override
		public Column<byte[]> previous() {
			if(currIndex==0)
				return null;
			return columns.getColumnByIndex(currIndex--);
		}

		@Override
		public int previousIndex() {
			return currIndex-1;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("We don't support this");
			
		}

		@Override
		public void set(Column<byte[]> arg0) {
			throw new UnsupportedOperationException("We don't support this");
			
		}
		
	}

}