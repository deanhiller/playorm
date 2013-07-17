package com.alvazan.orm.layer5.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.z5api.NoSqlSession;
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
import com.alvazan.orm.api.z8spi.action.RemoveEnum;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.IterableWrappingCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	private List<Action> actions = new ArrayList<Action>();
	private MetaLookup ormSession;
	private List<byte[]> newTables = new ArrayList<byte[]>();
	
	@Override
	public void put(DboTableMeta colFamily, byte[] rowKey, List<Column> columns) {
		if(DboTableMeta.class.getSimpleName().equals(colFamily))
			newTables.add(rowKey);

		Persist persist = new Persist();
		persist.setColFamily(colFamily);
		persist.setRowKey(rowKey);
		persist.setColumns(columns);
		actions.add(persist);
	}

	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey) {
		Remove remove = new Remove();
		remove.setAction(RemoveEnum.REMOVE_ENTIRE_ROW);
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		actions.add(remove);			
	}
	
	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey, Collection<byte[]> columnNames) {
		Remove remove = new Remove();
		remove.setAction(RemoveEnum.REMOVE_COLUMNS_FROM_ROW);
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		remove.setColumns(columnNames);
		actions.add(remove);		
	}

	@Override
	public void persistIndex(DboTableMeta cf, String indexColFamily, byte[] rowKey, IndexColumn column) {
		PersistIndex persist = new PersistIndex();
		persist.setColFamily(cf);
		persist.setIndexCfName(indexColFamily);
		persist.setRowKey(rowKey);
		persist.setColumn(column);
		actions.add(persist);
	}

	@Override
	public void removeColumn(DboTableMeta colFamily, byte[] rowKey, byte[] columnName) {
		RemoveColumn removeColumn = new RemoveColumn();
     	removeColumn.setColFamily(colFamily);
     	removeColumn.setRowKey(rowKey);
     	removeColumn.setColumn(columnName);
		actions.add(removeColumn);
	}

	@Override
	public void removeFromIndex(DboTableMeta cf, String indexColFamily, byte[] rowKeyBytes,
			IndexColumn c) {
		RemoveIndex remove = new RemoveIndex();
		remove.setColFamily(cf);
		remove.setIndexCfName(indexColFamily);
		remove.setRowKey(rowKeyBytes);
		remove.setColumn(c);
		actions.add(remove);
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, boolean skipCache, boolean cacheResults, Integer batchSize) {
		int size = 500;
		if(batchSize != null)
			size = batchSize;
		return rawSession.find(colFamily, rowKeys, null, size, null, ormSession);
	}
	
	private List<Row> find(DboTableMeta colFamily, List<byte[]> keys, boolean cacheResults) {
		AbstractCursor<KeyValue<Row>> results = find(colFamily, new IterableWrappingCursor<byte[]>(keys), false, cacheResults, null);
		List<Row> rows = new ArrayList<Row>();
		while(true) {
			Holder<KeyValue<Row>> holder = results.nextImpl();
			if(holder == null)
				break;
			KeyValue<Row> kv = holder.getValue();
			rows.add(kv.getValue());
		}
		return rows;
	}
	
	public Row find(DboTableMeta colFamily, byte[] rowKey) {
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		//log.debug("cf="+colFamily+" finding the key="+new ByteArray(rowKey));
		List<Row> rows = find(colFamily, rowKeys, true);
		return rows.get(0);
	}

	@Override
	public void flush() {
		//if(log.isDebugEnabled())
		//	logInformation();
		rawSession.sendChanges(actions, ormSession);
		actions = new ArrayList<Action>();
		
		//special case here...if any persists were of the DboTableMeta, we should create table now
		for(byte[] key : newTables) {
			String colFamily = StandardConverters.convertFromBytes(String.class, key);
			rawSession.readMetaAndCreateTable(ormSession, colFamily);
		}
		newTables.clear();
	}

//	private void insertTime(Action action, long time) {
//		if(action instanceof Persist) {
//			((Persist)action).setTimestamp(time);
//		}
//	}

	@Override
	public NoSqlRawSession getRawSession() {
		return rawSession;
	}

	@Override
	public void clearDb() {
		rawSession.clearDatabase();
	}
	@Override
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize) {
		return rawSession.columnSlice(colFamily, rowKey, from, to, batchSize, null, ormSession);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return rawSession.scanIndex(info, from, to, batchSize, null, ormSession);
	}
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values) {
		return rawSession.scanIndex(scanInfo, values, null, ormSession);
	}
	
	@Override
	public void setOrmSessionForMeta(MetaLookup session) {
		this.ormSession = session;
	}

	@Override
	public void clear() {
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, int batchSize) {
		return rawSession.allRows(colFamily, ormSession, batchSize);
	}
}
