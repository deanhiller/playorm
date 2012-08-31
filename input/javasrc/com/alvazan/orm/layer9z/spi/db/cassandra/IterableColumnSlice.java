package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession.CreateColumnSliceCallback;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

class IterableColumnSlice<T> implements Iterable<T> {

	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener bListener;
	private Integer batchSize;

	public IterableColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener, Integer batchSize) {
		this.callback = l;
		this.isComposite = isComposite;
		this.bListener = bListener;
		this.batchSize = batchSize;
	}

	@Override
	public Iterator<T> iterator() {
		//For some dang reason with current version of astyanax have to recreate this or nested join loop not working as when
		//we iterate a second time, we get 5 results when first time we got the correct 3 results...weird
		RowQuery<byte[], byte[]> rowQuery = callback.createRowQuery();
		
		return new OurIterator<T>(rowQuery, isComposite, bListener, batchSize);
	}
	
	private static class OurIterator<T> implements Iterator<T> {
		private RowQuery<byte[], byte[]> query;
		private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
		private boolean isComposite;
		private BatchListener batchListener;
		private int count;
		private Integer batchSize;
		
		public OurIterator(RowQuery<byte[], byte[]> query, boolean isComposite, BatchListener bListener, Integer batchSize) {
			this.query = query;
			this.isComposite = isComposite;
			this.batchListener = bListener;
			this.batchSize = batchSize;
		}

		@Override
		public boolean hasNext() {
			fetchMoreResults();
			if(subIterator == null)
				return false;
			
			boolean has = subIterator.hasNext();
			return has;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T next() {
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

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
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
}