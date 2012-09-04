package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession.CreateColumnSliceCallback;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

class CursorColumnSlice<T> extends AbstractCursor<T> {

	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener batchListener;
	private Integer batchSize;

	private RowQuery<byte[], byte[]> query;
	private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
	private int count;
	
	public CursorColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener, Integer batchSize) {
		this.callback = l;
		this.isComposite = isComposite;
		this.batchListener = bListener;
		this.batchSize = batchSize;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		//FOR some reason in astyanax, we have to keep getting the row query on resets or this Cursor returns 5 rows instead of the
		//SAME two rows it returned the first time.....well, form one of the unit tests
		 query = callback.createRowQuery();
		 subIterator = null;
	}

	@Override
	public Holder<T> nextImpl() {
		try {
			Iterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator = fetchMoreResultsImpl();
			if(latestIterator == null)
				return null;
			return nextColumn(latestIterator);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Holder<T> nextColumn(Iterator<com.netflix.astyanax.model.Column<byte[]>> latestIterator) {
		com.netflix.astyanax.model.Column<byte[]> col = latestIterator.next();
		count++;
		
		if(isComposite) {
			IndexColumn c = convertToIndexCol(col);
			return new Holder<T>((T) c);
		} else {
			Object obj = col.getName();
			Column c = new Column();
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
	
	
	private Iterator<com.netflix.astyanax.model.Column<byte[]>> fetchMoreResultsImpl() throws ConnectionException {
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
		batchListener.beforeFetchingNextBatch();
		OperationResult<ColumnList<byte[]>> opResult = query.execute();
		ColumnList<byte[]> columns = opResult.getResult();

		if(columns.isEmpty())
			subIterator = null; 
		else 
			subIterator = columns.iterator();
		
		batchListener.afterFetchingNextBatch(columns.size());
		return subIterator;
	}


}