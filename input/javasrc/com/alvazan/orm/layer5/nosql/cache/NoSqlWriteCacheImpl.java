package com.alvazan.orm.layer5.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.AbstractCursor;
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
import com.alvazan.orm.api.z8spi.action.RemoveEnum;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	private List<Action> actions = new ArrayList<Action>();
	private MetaLookup ormSession;
	
	@Override
	public void put(String colFamily, byte[] rowKey, List<Column> columns) {
		Persist persist = new Persist();
		persist.setColFamily(colFamily);
		persist.setRowKey(rowKey);
		persist.setColumns(columns);
		actions.add(persist);
	}

	@Override
	public void remove(String colFamily, byte[] rowKey) {
		Remove remove = new Remove();
		remove.setAction(RemoveEnum.REMOVE_ENTIRE_ROW);
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		actions.add(remove);			
	}
	
	@Override
	public void remove(String colFamily, byte[] rowKey, Collection<byte[]> columnNames) {
		Remove remove = new Remove();
		remove.setAction(RemoveEnum.REMOVE_COLUMNS_FROM_ROW);
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		remove.setColumns(columnNames);
		actions.add(remove);		
	}

	@Override
	public void persistIndex(String cf, String indexColFamily, byte[] rowKey, IndexColumn column) {
		PersistIndex persist = new PersistIndex();
		persist.setColFamily(indexColFamily);
		persist.setRealColFamily(cf);
		persist.setRowKey(rowKey);
		persist.setColumn(column);
		actions.add(persist);
	}	
	
	@Override
	public void removeFromIndex(String cf, String indexColFamily, byte[] rowKeyBytes,
			IndexColumn c) {
		RemoveIndex remove = new RemoveIndex();
		remove.setColFamily(indexColFamily);
		remove.setRealColFamily(cf);
		remove.setRowKey(rowKeyBytes);
		remove.setColumn(c);
		actions.add(remove);
	}
	
	@Override
	public AbstractCursor<KeyValue<Row>> findAll(String colFamily, Iterable<byte[]> rowKeys, boolean skipCache) {
		Iterable<KeyValue<Row>> iterable = rawSession.find(colFamily, rowKeys);
		return new ProxyTempCursor<KeyValue<Row>>(iterable);
	}
	
	public List<Row> find(String colFamily, List<byte[]> keys) {
		Iterable<KeyValue<Row>> results = rawSession.find(colFamily, keys);
		List<Row> rows = new ArrayList<Row>();
		for(KeyValue<Row> kv : results) {
			rows.add(kv.getValue());
		}
		return rows;
	}
	
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		//log.debug("cf="+colFamily+" finding the key="+new ByteArray(rowKey));
		List<Row> rows = find(colFamily, rowKeys);
		return rows.get(0);
	}

	@Override
	public void flush() {
		//if(log.isDebugEnabled())
		//	logInformation();
		
		rawSession.sendChanges(actions, ormSession);
		actions = new ArrayList<Action>();
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
	public AbstractCursor<Column> columnSlice(String colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize) {
		return rawSession.columnSlice(colFamily, rowKey, from, to, batchSize, null);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return rawSession.scanIndex(info, from, to, batchSize, null);
	}
	
	@Override
	public void setOrmSessionForMeta(MetaLookup session) {
		this.ormSession = session;
	}

}
