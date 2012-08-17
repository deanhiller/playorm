package com.alvazan.orm.layer5.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.Action;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Persist;
import com.alvazan.orm.api.spi9.db.PersistIndex;
import com.alvazan.orm.api.spi9.db.Remove;
import com.alvazan.orm.api.spi9.db.RemoveEnum;
import com.alvazan.orm.api.spi9.db.RemoveIndex;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	private List<Action> actions = new ArrayList<Action>();
	private Object ormSession;
	
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
	public List<Row> find(String colFamily, List<byte[]> keys) {
		List<ByteArray> theKeys = new ArrayList<ByteArray>();
		for(byte[] k : keys) {
			theKeys.add(new ByteArray(k));
		}
		//log.debug("cf="+colFamily+" finding all keys="+theKeys);
		return rawSession.find(colFamily, keys);
	}
	
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		//log.debug("cf="+colFamily+" finding the key="+new ByteArray(rowKey));
		List<Row> rows = rawSession.find(colFamily, rowKeys);
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
	public Iterable<Column> columnRangeScan(ScanInfo info, Key from, Key to, int batchSize) {
		return rawSession.columnRangeScan(info, from, to, batchSize);
	}
	
	@Override
	public void setOrmSessionForMeta(Object session) {
		this.ormSession = session;
	}

}
