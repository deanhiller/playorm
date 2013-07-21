package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ScanCassandraCfAllRows extends AbstractCursor<com.alvazan.orm.api.z8spi.Row> {

	private static final Logger log = LoggerFactory.getLogger(ScanCassandraCfAllRows.class);

	private Keyspace keyspace;
	private Integer batchSize;
	private Info cfInfo;

	private boolean reversed;
	private Iterator<Row<byte[], byte[]>> iterator;

	private Provider<com.alvazan.orm.api.z8spi.Row> rowProvider;

	public ScanCassandraCfAllRows(Info info1, Integer batchSize, Keyspace keyspace, Provider<com.alvazan.orm.api.z8spi.Row> rowProvider) {
		this.keyspace = keyspace;
		this.cfInfo = info1;
		this.batchSize = batchSize;
		this.rowProvider = rowProvider;
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
	public Holder<com.alvazan.orm.api.z8spi.Row> nextImpl() {
		if(reversed)
			throw new IllegalStateException("Must call beforeFirst and next, next.  You cannot call afterLast and then use the next function");
		return fetchNext();
	}

	@Override
	public Holder<com.alvazan.orm.api.z8spi.Row> previousImpl() {
		if(!reversed)
			throw new IllegalStateException("Must call afterLast and previous, previoust.  You cannot user previous function unless you start at the end");
		return fetchNext();
	}

	private Holder<com.alvazan.orm.api.z8spi.Row> fetchNext() {
		if(!iterator.hasNext())
			return null;

		Row<byte[], byte[]> row = iterator.next();
		byte[] key = row.getKey();

		com.alvazan.orm.api.z8spi.Row r = rowProvider.get();
		r.setKey(key);
		CassandraSession.processColumns(row, r);
		
		return new Holder<com.alvazan.orm.api.z8spi.Row>(r);
	}
}
