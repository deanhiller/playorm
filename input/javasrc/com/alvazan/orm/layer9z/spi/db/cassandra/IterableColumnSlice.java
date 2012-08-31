package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession.CreateColumnSliceCallback;
import com.alvazan.orm.util.AbstractCursor;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

class IterableColumnSlice<T> extends AbstractCursor<T> {

	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener batchListener;
	private Integer batchSize;

	private RowQuery<byte[], byte[]> query;
	private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
	private int count;
	
	public IterableColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener, Integer batchSize) {
		this.callback = l;
		this.isComposite = isComposite;
		this.batchListener = bListener;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		//FOR some reason in astyanax, we have to keep getting the row query on resets or this Cursor returns 5 rows instead of the
		//SAME two rows it returned the first time.....well, form one of the unit tests
		 query = callback.createRowQuery();
	}

	@Override
	protected com.alvazan.orm.util.AbstractCursor.Holder<T> nextImpl() {
		fetchMoreResults();
		if(subIterator == null)
			return null;
		
		T col = nextColumn();
		return new Holder<T>(col);
	}

	@SuppressWarnings("unchecked")
	public T nextColumn() {
		fetchMoreResults();
		if(subIterator == null)
			throw new ArrayIndexOutOfBoundsException("no more elements");

		com.netflix.astyanax.model.Column<byte[]> col = subIterator.next();
		count++;
		
		Object obj = col.getName();
		if(isComposite) {
			GenericComposite bigDec = (GenericComposite)obj;
			IndexColumn c = new IndexColumn();
			c.setPrimaryKey(bigDec.getPk());
			c.setIndexedValue(bigDec.getIndexedValue());
			c.setValue(col.getByteArrayValue());
			return (T) c;
		} else {
			Column c = new Column();
			byte[] name = (byte[])obj;
			c.setName(name);
			c.setValue(col.getByteArrayValue());
			c.setTimestamp(col.getTimestamp());
			return (T) c;
		}
	}
	
	
	private void fetchMoreResults() {
		try {
			fetchMoreResultsImpl();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void fetchMoreResultsImpl() throws ConnectionException {
		if(subIterator != null){
			//If subIterator is not null, we have already previously fetched results!!!
			if(subIterator.hasNext())
				return; //no need to fetch more since subIterator has more
			else if(batchSize == null) //then we already fetched everything in first round
				return;
			else if(count < batchSize - 1) {
				//since we have previous results, we then have a count of those results BUT they
				//did NOT fill up the batch so no need to go to database as we know it has no more results
				return;
			}
		}
		
		count = 0;
		batchListener.beforeFetchingNextBatch();
		ColumnList<byte[]> columns = query.execute().getResult();

		if(columns.isEmpty())
			subIterator = null; 
		else 
			subIterator = columns.iterator();
		
		batchListener.afterFetchingNextBatch(columns.size());
	}


}