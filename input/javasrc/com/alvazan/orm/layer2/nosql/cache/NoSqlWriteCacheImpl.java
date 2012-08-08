package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Action;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Persist;
import com.alvazan.orm.api.spi3.db.PersistIndex;
import com.alvazan.orm.api.spi3.db.Remove;
import com.alvazan.orm.api.spi3.db.RemoveEnum;
import com.alvazan.orm.api.spi3.db.RemoveIndex;
import com.alvazan.orm.api.spi3.db.Row;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	private NoSqlRawSession rawSession;
	private List<Action> actions = new ArrayList<Action>();
	private Object ormSession;
	
	@Override
	public void persist(String colFamily, byte[] rowKey, List<Column> columns) {
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
	public void persistIndex(String colFamily, byte[] rowKey, IndexColumn column) {
		PersistIndex persist = new PersistIndex();
		persist.setColFamily(colFamily);
		persist.setRowKey(rowKey);
		persist.setColumn(column);
		actions.add(persist);
	}	
	
	@Override
	public void removeFromIndex(String columnFamilyName, byte[] rowKeyBytes,
			IndexColumn c) {
		RemoveIndex remove = new RemoveIndex();
		remove.setColFamily(columnFamilyName);
		remove.setRowKey(rowKeyBytes);
		remove.setColumn(c);
		actions.add(remove);
	}
	
	@Override
	public List<Row> find(String colFamily, List<byte[]> keys) {
		return rawSession.find(colFamily, keys);
	}
	
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		List<Row> rows = find(colFamily, rowKeys);
		return rows.get(0);
	}

	@Override
	public void flush() {
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
	public Iterable<Column> columnRangeScan(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) {
		return rawSession.columnRangeScan(colFamily, rowKey, from, to, batchSize);
	}

	@Override
	public void setOrmSessionForMeta(Object session) {
		this.ormSession = session;
	}

}
