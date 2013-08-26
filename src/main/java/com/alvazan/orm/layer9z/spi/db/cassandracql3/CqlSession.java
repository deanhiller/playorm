package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public class CqlSession implements NoSqlRawSession {
	private Session session = null;
	private Cluster cluster = null;

	@Override
	public void sendChanges(List<Action> actions, MetaLookup session) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearDatabase() {
		// TODO Auto-generated method stub

	}

	 @Override
		public void start(Map<String, Object> properties) {
		    String cqlSeed = "localhost";
		    String keys="newKeyspace";
			cluster = Cluster.builder().addContactPoint(cqlSeed).build();
			session = cluster.connect();
			List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
			int count = 0;
			for (KeyspaceMetadata ks : keyspaces) {
				if (ks.getName().equalsIgnoreCase(keys)) {
					count++;
				}
			}
			if (count == 0) {
				session.execute("CREATE KEYSPACE "+keys+" WITH replication "
						+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
				} 
			Session session = cluster.connect(keys);
				
		}
		   

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public AbstractCursor<Column> columnSlice(ColumnSliceInfo sliceInfo,
			Integer batchSize, BatchListener l, MetaLookup mgr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from,
			Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo,
			List<byte[]> values, BatchListener list, MetaLookup mgr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache cache, int batchSize,
			BatchListener list, MetaLookup mgr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
		// TODO Auto-generated method stub

	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, MetaLookup mgr,
			int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getDriverHelper() {
		// TODO Auto-generated method stub
		return null;
	}

}