package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import com.alvazan.orm.api.spi9.db.BatchListener;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession.CreateColumnSliceCallback;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;

class IterableColumnSlice<T> implements Iterable<T> {

	private CreateColumnSliceCallback callback;
	private boolean isComposite;
	private BatchListener bListener;

	public IterableColumnSlice(CreateColumnSliceCallback l, boolean isComposite, BatchListener bListener) {
		this.callback = l;
		this.isComposite = isComposite;
		this.bListener = bListener;
	}

	@Override
	public Iterator<T> iterator() {
		//For some dang reason with current version of astyanax have to recreate this or nested join loop not working as when
		//we iterate a second time, we get 5 results when first time we got the correct 3 results...weird
		RowQuery<byte[], byte[]> rowQuery = callback.createRowQuery();
		
		return new OurIterator<T>(rowQuery, isComposite, bListener);
	}
	
	private static class OurIterator<T> implements Iterator<T> {
		private RowQuery<byte[], byte[]> query;
		private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
		private boolean isComposite;
		private BatchListener batchListener;
		
		public OurIterator(RowQuery<byte[], byte[]> query, boolean isComposite, BatchListener bListener) {
			this.query = query;
			this.isComposite = isComposite;
			this.batchListener = bListener;
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
			if(subIterator != null && subIterator.hasNext())
				return; //no need to fetch more since subIterator has more

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