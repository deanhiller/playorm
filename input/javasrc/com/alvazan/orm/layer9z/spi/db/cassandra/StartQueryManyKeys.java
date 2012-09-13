package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class StartQueryManyKeys implements StartQueryListener {

	private ScanInfo scanInfo;
	private List<byte[]> values;
	private Info info1;
	private ColumnFamilyHelper columnFamilies;
	
	public StartQueryManyKeys(ColumnFamilyHelper columnFamilies, Info info1, ScanInfo info, List<byte[]> values) {
		this.columnFamilies = columnFamilies;
		this.info1 = info1;
		this.scanInfo = info;
		this.values = values;
	}

	@Override
	public List<Future<OperationResult<ColumnList<byte[]>>>> start() {
		ColumnFamily cf = info1.getColumnFamilyObj();
		Keyspace keyspace = columnFamilies.getKeyspace();
		
		List<Future<OperationResult<ColumnList<byte[]>>>> futures = new ArrayList<Future<OperationResult<ColumnList<byte[]>>>>();
		for(byte[] val : values) {
			Key from = new Key(val, true);
			Key to = new Key(val, true);
			byte[] rowKey = scanInfo.getRowKey();
			ByteArray valB = new ByteArray(val);
			ByteArray forRow = new ByteArray(rowKey);
			
			CompositeRangeBuilder range = CassandraSession.setupRangeBuilder(from, to, info1);
			ColumnFamilyQuery query = keyspace.prepareQuery(cf);
			RowQuery<byte[], byte[]> rowQuery = query.getKey(rowKey).withColumnRange(range);
			Future future = executeAsync(rowQuery);
			futures.add(future);
		}
		
		return futures;
	}

	private Future executeAsync(RowQuery rowQuery) {
		try {
			return rowQuery.executeAsync();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
}
