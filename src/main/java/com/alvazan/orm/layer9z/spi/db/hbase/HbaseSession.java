package com.alvazan.orm.layer9z.spi.db.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Bootstrap;
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
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveColumn;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseSession implements NoSqlRawSession {

	/** The conf. */
	private HBaseConfiguration conf;

	/** The h table pool. */
	private HTablePool hTablePool;

	private HTableInterface hTable;

	private HBaseAdmin hAdmin;
	// private HTable db;
	@Inject
	private Provider<Row> rowProvider;
	@Inject
	private DboDatabaseMeta dbMetaFromHbaseOrmOnly;

	private HTableDescriptor tableDescriptor;
	private Map<String, Info> cfNameToHbase = new HashMap<String, Info>();
	private Map<String, String> virtualToCfHbaseName = new HashMap<String, String>();

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
		byte[] rowKey = action.getRowKey();
		if (action.getAction() == null)
			throw new IllegalArgumentException("action param is missing ActionEnum so we know to remove entire row or just columns in the row");
		switch (action.getAction()) {
		case REMOVE_ENTIRE_ROW:
			Delete delete = new Delete(rowKey);
			try {
				hTable.delete(delete);
			} catch (IOException e) {

				e.printStackTrace();
			}
			break;
		case REMOVE_COLUMNS_FROM_ROW:
			removeColumns(action, ormSession);
			break;
		default:
			throw new RuntimeException("bug, unknown remove action="
					+ action.getAction());
		}
	}

	private void removeColumns(Remove action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		Info info = lookupOrCreate(colFamily, ormSession);
		HColumnDescriptor hColumnDescriptor = info.getColFamily();
		String tableName = hColumnDescriptor.getNameAsString();
		byte[] colFamilyBytes = Bytes.toBytes(tableName);
		Collection<byte[]> columns = action.getColumns();
		List<Delete> listDelete = new ArrayList<Delete>();
		for (byte[] col : columns) {
			Delete delete = new Delete(action.getRowKey());
			delete.deleteColumns(colFamilyBytes, col);
			listDelete.add(delete);
		}
		try {
			hTable.delete(listDelete);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
		String indexCfName = action.getIndexCfName();
		if (indexCfName.equalsIgnoreCase("BytesIndice"))
			return;
		byte[] family = Bytes.toBytes(indexCfName);
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		byte[] value = column.getPrimaryKey();
		Delete delete = new Delete(rowKey);
		delete.deleteColumn(family, value);
		try {
			hTable.delete(delete);
			hTable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void removeColumn(RemoveColumn action, MetaLookup ormSession) {
		Info info = lookupOrCreate(action.getColFamily().getColumnFamily(), ormSession);
		HColumnDescriptor hColumnDescriptor=info.getColFamily();
		String tableName = hColumnDescriptor.getNameAsString();
		byte[] colFamily = Bytes.toBytes(tableName);
		byte[] c = action.getColumn();
		Delete delete = new Delete(action.getRowKey());
		delete.deleteColumns(colFamily, c);
		try {
			hTable.delete(delete);
			hTable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void persistIndex(PersistIndex action, MetaLookup ormSession) {
		String indexCfName = action.getIndexCfName();
		Info info = lookupOrCreate(indexCfName, ormSession);
		HColumnDescriptor hColFamily = info.getColFamily();
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		byte[] key = column.getIndexedValue();
		byte[] value = column.getPrimaryKey();
		Get get = new Get(rowKey);
		Object keyToPersist = null;
		byte[] byteArr = null;
		if (key != null) {
			if ((indexCfName.equalsIgnoreCase("IntegerIndice"))) {
				keyToPersist = StandardConverters.convertFromBytes(Integer.class, key);
				int tempInt = ((Integer) keyToPersist).intValue();
				tempInt ^= (1 << 31);
				byteArr = Bytes.toBytes(tempInt);
			} else
				byteArr = key;
		}

		try {
			Result result = hTable.get(get);
			byte[] existing = result.getValue(hColFamily.getName(), value);
			if (existing != null) {
				if (!Bytes.equals(existing, byteArr)) {
					Put put = new Put(rowKey);
					put.add(hColFamily.getName(), value, byteArr);
					hTable.put(put);
					hTable.flushCommits();
				}
			} else {
				Put put = new Put(rowKey);
				put.add(hColFamily.getName(), value, byteArr);
				hTable.put(put);
				hTable.flushCommits();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void clearDatabase() {
		try {
			String tableName = tableDescriptor.getNameAsString();
			HTableDescriptor td = hAdmin.getTableDescriptor(Bytes.toBytes(tableName));
			if (hAdmin.isTableEnabled(tableName))
				hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
			hAdmin.createTable(td);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void start(Map<String, Object> properties) {
		properties.keySet();
		org.apache.hadoop.conf.Configuration hadoopConf = HBaseConfiguration
				.create();
		String hbaseSeed = properties.get(Bootstrap.HBASE_SEEDS)
				.toString();
		hadoopConf.set("hbase.zookeeper.quorum", hbaseSeed);
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
	public AbstractCursor<Column> columnSlice(ColumnSliceInfo sliceInfo, Integer batchSize,BatchListener l, MetaLookup mgr) {
		Info info1 = lookupOrCreate(sliceInfo.getColFamily().getColumnFamily(), mgr);
		if (info1 == null) {
			return null;
		}
		CursorColumnSliceHbase cursor = new CursorColumnSliceHbase(sliceInfo, l, batchSize, hTable, info1.getColFamily());
		return cursor;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from,
			Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		byte[] rowKey = scan.getRowKey();
		String indexTableName = scan.getIndexColFamily();
		DboColumnMeta colMeta = scan.getColumnName();
		CursorOfHbaseIndexes cursor = new CursorOfHbaseIndexes(rowKey, batchSize, l, indexTableName, from, to);
		cursor.setupMore(hTable, colMeta);
		if (!scan.getEntityColFamily().isVirtualCf() && from == null
				&& to == null
				&& !(scan.getColumnName() instanceof DboColumnToManyMeta)
				&& !scan.getEntityColFamily().isInheritance()) {
			String table = scan.getEntityColFamily().getColumnFamily();
			ScanHbaseDbCollection scanner = new ScanHbaseDbCollection(batchSize, l,table, hTable);
			scanner.beforeFirst();
			return scanner;
		}
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
		StorageTypeEnum type = action.getColFamily().getNameStorageType();
		String colFamily = action.getColFamily().getColumnFamily();
		Info info = lookupOrCreate(colFamily, ormSession);
		HColumnDescriptor hColFamily = info.getColFamily();
		byte[] rowKey = action.getRowKey();
		Put put = new Put(rowKey);
		try {
			if (type == StorageTypeEnum.INTEGER) {
				for (Column col : action.getColumns()) {
					byte[] qualifier = new byte[0];
					qualifier = col.getName();
					int fillipedNumber = StandardConverters.convertFromBytes(
							Integer.class, qualifier);
					fillipedNumber ^= (1 << 31);
					qualifier = Bytes.toBytes(fillipedNumber);
					byte[] value = new byte[0];
					if (col.getValue() != null) {
						value = col.getValue();
					}
					put.add(hColFamily.getName(), qualifier, value);
					hTable.put(put);
				}

			} else {
				for (Column col : action.getColumns()) {
					byte[] qualifier = new byte[0];
					qualifier = col.getName();
					byte[] value = new byte[0];
					if (col.getValue() != null) {
						value = col.getValue();
					}
					put.add(hColFamily.getName(), qualifier, value);
					hTable.put(put);
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			hTable.flushCommits();
			// hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	public Info lookupOrCreate(String virtualCf, MetaLookup ormSession) {
		Info origInfo = fetchDbCollectionInfo(virtualCf, ormSession);
		Exception ee = null;
		if (origInfo == null) {
			ee = createColFamily(virtualCf, ormSession);
		}
		Info info = fetchDbCollectionInfo(virtualCf, ormSession);
		if (info == null)
			throw new RuntimeException(
					"Could not create and could not find virtual or real colfamily="
							+ virtualCf
							+ " see chained exception AND it could be your name is not allowed as a valid Hbase Column Family name",
					ee);
		return info;
	}

	private Info fetchDbCollectionInfo(String tableName, MetaLookup mgr) {
		Info info = lookupVirtCf(tableName);
		if (info != null) {
			return info;
		}
		return tryToLoadColumnFamilyVirt(tableName, mgr);
	}

	private Info tryToLoadColumnFamilyVirt(String virtColFamily,
			MetaLookup lookup) {
		Info info = tryToLoadColumnFamilyImpl(virtColFamily, lookup);
		return info;
	}

	private Info tryToLoadColumnFamilyImpl(String virtCf, MetaLookup lookup) {
		synchronized (virtCf.intern()) {
			String cfName = virtualToCfHbaseName.get(virtCf);
			if (cfName != null) {
				Info info = cfNameToHbase.get(cfName);
				if (info != null) {
					return cfNameToHbase.get(cfName);
				}
			}
			DboTableMeta table = loadFromInMemoryOrDb(virtCf, lookup);
			String realCf = table.getRealColumnFamily();
			String realCfLower = realCf.toLowerCase();
			Info info = cfNameToHbase.get(realCfLower);
			String table1 = tableDescriptor.getNameAsString();
			if (info != null) {
				String cfLowercase = realCf.toLowerCase();
				virtualToCfHbaseName.put(virtCf, cfLowercase);
				return info;
			}
			try {
				if (hAdmin != null && hAdmin.tableExists(table1)) {
					hTable = hTablePool.getTable(table1);
					loadColumnFamily(hTable, virtCf, realCf);
					return lookupVirtCf(virtCf);
				} else {
					return null;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private void loadColumnFamily(HTableInterface def, String virtCf,
			String realCf) {
		addExistingCollections(def);
		virtualToCfHbaseName.put(virtCf, realCf);
	}

	private void addExistingCollections(HTableInterface def) {
		HColumnDescriptor[] columnFamilies;
		try {
			columnFamilies = def.getTableDescriptor().getColumnFamilies();
			for (HColumnDescriptor columnFamily : columnFamilies) {
				Info info = createInfo(columnFamily, null, null);
				cfNameToHbase.put(columnFamily.getNameAsString(), info);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private synchronized Exception createColFamily(String virtualCf,
			MetaLookup ormSession) {
		try {
			createColFamilyImpl(virtualCf, ormSession);
		} catch (Exception e) {
			return e;
		}
		return null;
	}

	private synchronized void createColFamilyImpl(String virtualCf,
			MetaLookup ormSession) {
		if (lookupVirtCf(virtualCf) != null)
			return;
		DboTableMeta meta = loadFromInMemoryOrDb(virtualCf, ormSession);
		createColFamilyInHbase(meta);
	}

	private void createColFamilyInHbase(DboTableMeta meta) {
		String colFamily = meta.getRealColumnFamily();
		byte[] family = Bytes.toBytes(colFamily);
		String tableName = tableDescriptor.getNameAsString();
		Info info = new Info();
		if (tableDescriptor.hasFamily(family)) {
			hTablePool.getTable(tableName);
		} else {
			try {
				if (hAdmin.isTableEnabled(tableName))
					hAdmin.disableTable(tableName);
				HColumnDescriptor colDescriptor = new HColumnDescriptor(
						colFamily);
				hAdmin.addColumn(tableName, colDescriptor);
				tableDescriptor.addFamily(colDescriptor);
				hAdmin.enableTable(tableName);
				info = createInfo(colDescriptor, null, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		String virtual = meta.getColumnFamily();
		String realCf = meta.getRealColumnFamily();
		String realCfLower = realCf.toLowerCase();
		virtualToCfHbaseName.put(virtual, realCfLower);
		cfNameToHbase.put(realCfLower, info);
	}

	private Info createInfo(HColumnDescriptor colFamily, Object type,
			Object keyType) {
		Info info = new Info();
		info.setColFamily(colFamily);
		return info;
	}

	private Info lookupVirtCf(String virtualCf) {
		String cfName = virtualToCfHbaseName.get(virtualCf);
		if (cfName == null)
			return null;
		Info info = cfNameToHbase.get(cfName);
		return info;
	}

	private DboTableMeta loadFromInMemoryOrDb(String virtCf, MetaLookup lookup) {
		DboTableMeta meta = dbMetaFromHbaseOrmOnly.getMeta(virtCf);
		if (meta != null)
			return meta;
		DboTableMeta table = lookup.find(DboTableMeta.class, virtCf);
		if (table == null)
			throw new IllegalArgumentException(
					"We can't load the meta for virtual or real CF="
							+ virtCf
							+ " because there is not meta found in DboTableMeta table");
		return table;
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, MetaLookup ormSession, int batchSize) {
		throw new UnsupportedOperationException("not supported yet");
	}

	@Override
	public Object getDriverHelper() {
		throw new UnsupportedOperationException("not supported yet");
	}

}