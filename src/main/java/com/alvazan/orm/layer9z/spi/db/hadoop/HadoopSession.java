package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveColumn;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;




import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HadoopSession implements NoSqlRawSession {

	/** The conf. */
	private HBaseConfiguration conf;

	/** The h table pool. */
	private HTablePool hTablePool;

	private HTableInterface hTable;

	private HBaseAdmin hAdmin;
	// private HTable db;
	@Inject
	private Provider<Row> rowProvider;

	private HTableDescriptor tableDescriptor;

	/** The pool size. */
	private int poolSize = 100;

	@Override
	public void sendChanges(List<Action> actions, MetaLookup ormSession) {

		for (Action action : actions) {
			if (action instanceof Persist) {
				persist((Persist) action, ormSession);
			} else if (action instanceof Remove) {
				remove((Remove) action, ormSession);
			} else if (action instanceof PersistIndex) {
				persistIndex((PersistIndex) action, ormSession);
			} else if (action instanceof RemoveIndex) {
				removeIndex((RemoveIndex) action, ormSession);
			} else if (action instanceof RemoveColumn) {
				removeColumn((RemoveColumn) action, ormSession);
			}
		}
	}

	private void remove(Remove action, MetaLookup ormSession) {
		// TODO Auto-generated method stub

	}

	private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
		// TODO Auto-generated method stub

	}

	private void removeColumn(RemoveColumn action, MetaLookup ormSession) {
		// TODO Auto-generated method stub

	}

	private void persistIndex(PersistIndex action, MetaLookup ormSession) {
		String indexCfName = action.getIndexCfName();
		Info info = lookupOrCreate(indexCfName, ormSession);
		HColumnDescriptor hColFamily = info.getColFamily();
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		byte[] key = column.getIndexedValue();
		byte[] value = column.getPrimaryKey();
		Result row = findOrCreateRow(hColFamily, rowKey);
		Put put = new Put(rowKey);
		put.add(hColFamily.getName(), key, value);
		try {
			hTable.put(put);
			hTable.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void clearDatabase() {
		try {
			hAdmin.deleteTable(tableDescriptor.getName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void start(Map<String, Object> properties) {
		properties.keySet();
		org.apache.hadoop.conf.Configuration hadoopConf = HBaseConfiguration
				.create();
		hadoopConf.set("hbase.zookeeper.quorum", "localhost");
		hTablePool = new HTablePool(conf, poolSize);
		try {
			hAdmin = new HBaseAdmin(hadoopConf);
			String keySpace = properties.get(Bootstrap.HBASE_KEYSPACE)
					.toString();
			if (hAdmin.tableExists(keySpace)) {
				tableDescriptor = hAdmin
						.getTableDescriptor(keySpace.getBytes());
			} else {

				tableDescriptor = new HTableDescriptor(keySpace);
				hAdmin.createTable(tableDescriptor);
			}
			final String tableName = tableDescriptor.getNameAsString();
			hTable = hTablePool.getTable(tableName);
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			hAdmin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {

	}

	@Override
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily,
			byte[] rowKey, byte[] from, byte[] to, Integer batchSize,
			BatchListener l, MetaLookup mgr) {
		return null;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from,
			Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		byte[] rowKey = scan.getRowKey();
		String indexTableName = scan.getIndexColFamily();
		DboColumnMeta colMeta = scan.getColumnName();
		CursorOfHbaseIndexes cursor = new CursorOfHbaseIndexes(rowKey, batchSize, l, indexTableName, from, to);
		cursor.setupMore(hTable, colMeta);
		return cursor;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo,
			List<byte[]> values, BatchListener list, MetaLookup mgr) {
		byte[] rowKey = scanInfo.getRowKey();
		String indexTableName = scanInfo.getIndexColFamily();
		CursorForHbaseValues cursor = new CursorForHbaseValues(rowKey, list,
				indexTableName, values, hTable);
		return cursor;
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache cache, int batchSize,
			BatchListener list, MetaLookup mgr) {
		Info info = lookupOrCreate(colFamily.getColumnFamily(), mgr);
		if (info == null) {
			return new CursorReturnsEmptyRowsHbase(rowKeys);
		}
		CursorKeysToRowsHbase cursor = new CursorKeysToRowsHbase(rowKeys,
				batchSize, list, rowProvider);
		cursor.setupMore(hTable, colFamily, info, cache);
		return cursor;
	}

	private void persist(Persist action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		Info info = lookupOrCreate(colFamily, ormSession);
		HColumnDescriptor hColFamily = info.getColFamily();
		byte[] rowKey = action.getRowKey();
		Result row = findOrCreateRow(hColFamily, rowKey);
		for (Column col : action.getColumns()) {
			Put put = new Put(rowKey);
			byte[] value = new byte[0];
			if (col.getValue() != null)
				value = col.getValue();
			put.add(hColFamily.getName(), col.getName(), value);
			try {
				hTable.put(put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			hTable.flushCommits();
			// hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Info lookupOrCreate(String virtualCf, MetaLookup ormSession) {
		Info info = new Info();
		byte[] family = Bytes.toBytes(virtualCf);
		String tableName = tableDescriptor.getNameAsString();
		if (tableDescriptor.hasFamily(family)) {
			hTablePool.getTable(tableName);
			HColumnDescriptor colDescriptor = tableDescriptor.getFamily(family);
			info.setColFamily(colDescriptor);
		} else {
			try {
				if (hAdmin.isTableEnabled(tableName))
					hAdmin.disableTable(tableName);
				HColumnDescriptor colDescriptor = new HColumnDescriptor(
						virtualCf);
				hAdmin.addColumn(tableDescriptor.getNameAsString(),
						colDescriptor);
				tableDescriptor.addFamily(colDescriptor);
				info.setColFamily(colDescriptor);
				hAdmin.enableTable(tableName);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return info;
	}

	public Result findOrCreateRow(HColumnDescriptor hColFamily, byte[] key) {
		Get get = new Get(key);
		Result r = new Result();
		try {
			r = hTable.get(get);
			if (r == null) {
				Put put = new Put(key);
				hTable.put(put);
				hTable.flushCommits();
				r = hTable.get(get);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
}