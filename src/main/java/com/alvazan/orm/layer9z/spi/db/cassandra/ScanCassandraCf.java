package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.util.RangeBuilder;

public class ScanCassandraCf extends AbstractCursor<IndexColumn> {

	private static final Logger log = LoggerFactory.getLogger(ScanCassandraCf.class);

	private Keyspace keyspace;
	private ScanInfo info;
	private BatchListener bListener;
	private Integer batchSize;
	private Info cfInfo;

	private boolean reversed;
	private Iterator<Row<byte[], byte[]>> iterator;

	public ScanCassandraCf(ScanInfo info, Info info1, BatchListener bListener,
			Integer batchSize, Keyspace keyspace) {
		this.keyspace = keyspace;
		this.info = info;
		this.cfInfo = info1;
		this.bListener = bListener;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		reversed = false;
		initialize();
	}

	@Override
	public void afterLast() {
		reversed = true;
		initialize();
	}

	@SuppressWarnings("unchecked")
	private void initialize() {
		try {
			ByteBufferRange range = new RangeBuilder().setReversed(reversed).build();
			ColumnFamily<byte[], byte[]> cf = cfInfo.getColumnFamilyObj();

			ColumnFamilyQuery<byte[], byte[]> cfQuery = keyspace.prepareQuery(cf);
			AllRowsQuery<byte[], byte[]> query = cfQuery.getAllRows();
			query.withColumnRange(range)
				.setExceptionCallback(new ExcCallback());
			
			if(batchSize != null) {
				if(batchSize < 10)
					throw new RuntimeException("batchSize must be 10 or greater and preferably around 500 is good.");
				query.setRowLimit(batchSize);
			}

			OperationResult<Rows<byte[], byte[]>> opResult = query.execute();
			iterator = opResult.getResult().iterator();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ExcCallback implements ExceptionCallback {

		@Override
		public boolean onException(ConnectionException e) {
			log.warn("Exception processing row", e);
            try {
            	//example had this possibly to slow it down...
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
            	throw new RuntimeException(e1);
            }
			return false;
		}
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> nextImpl() {
		if(reversed)
			throw new IllegalStateException("Must call beforeFirst and next, next.  You cannot call afterLast and then use the next function");
		return fetchNext();
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		if(!reversed)
			throw new IllegalStateException("Must call afterLast and previous, previoust.  You cannot user previous function unless you start at the end");
		return fetchNext();
	}

	private com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> fetchNext() {
		if(!iterator.hasNext())
			return null;
		
		Row<byte[], byte[]> row = iterator.next();
		byte[] key = row.getKey();
		
		IndexColumn col = new IndexColumn();
		col.setPrimaryKey(key);
		return new Holder<IndexColumn>(col);
	}
}
