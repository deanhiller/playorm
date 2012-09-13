package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;

public class FindRowsCursor extends AbstractCursor<KeyValue<Row>> {

	private Info info;
	private Iterable<byte[]> rowKeys;
	private int batchSize;
	private BatchListener list;
	private Keyspace keyspace;
	private Iterator<byte[]> theKeys;
	private int lastRowCount;
	private Iterator<KeyValue<Row>> cachedRows;
	private Provider<Row> rowProvider;

	public FindRowsCursor(Info info, Iterable<byte[]> rowKeys, int batchSize,
			BatchListener list, Keyspace keyspace, Provider<Row> rowProvider) {
		this.rowProvider = rowProvider;
		this.info = info;
		this.rowKeys = rowKeys;
		this.batchSize = batchSize;
		this.list = list;
		this.keyspace = keyspace;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		theKeys = rowKeys.iterator();
		cachedRows = null;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		loadCache();
		if(!cachedRows.hasNext())
			return null;
		
		return new Holder<KeyValue<Row>>(cachedRows.next());
	}

	@SuppressWarnings("unchecked")
	private void loadCache() {
		if(cachedRows != null && cachedRows.hasNext())
			return; //There are more rows so return and the code will return the next result from cache
		else if(lastRowCount >= batchSize)
			return; //There will be now rows so don't waste time querying!!!!
			
		List<byte[]> allKeys = new ArrayList<byte[]>();
		for(int i = 0; i < batchSize && theKeys.hasNext(); i++) {
			byte[] key = theKeys.next();
			allKeys.add(key);
		}
		
		list.beforeFetchingNextBatch();
		
		ColumnFamily<byte[], byte[]> cf = info.getColumnFamilyObj();
		ColumnFamilyQuery<byte[], byte[]> q2 = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = q2.getKeySlice(allKeys);
		
		OperationResult<Rows<byte[], byte[]>> result = execute(slice);
		
		Rows<byte[], byte[]> rows = result.getResult();		
		Iterator<com.netflix.astyanax.model.Row<byte[], byte[]>> resultingRows = rows.iterator();
		lastRowCount = rows.size();
		list.afterFetchingNextBatch(rows.size());

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
		while(resultingRows.hasNext()) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = resultingRows.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(row.getKey());
			if(!row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				Row r = rowProvider.get();
				r.setKey(row.getKey());
				CassandraSession.processColumns(row, r);
				kv.setValue(r);
			}
			
			ByteArray b = new ByteArray(row.getKey());
			map.put(b, kv);
		}		
		
		//UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we provided so, we need to iterate over the whole thing here
		//into our own List :( :( .
		List<KeyValue<Row>> results = new ArrayList<KeyValue<Row>>();
		for(byte[] k : allKeys) {
			ByteArray b = new ByteArray(k);
			KeyValue<Row> kv = map.get(b);
			results.add(kv);
		}
		
		cachedRows = results.iterator();
	}

	private OperationResult<Rows<byte[], byte[]>> execute(
			RowSliceQuery<byte[], byte[]> slice) {
		try {
			return slice.execute();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

}
