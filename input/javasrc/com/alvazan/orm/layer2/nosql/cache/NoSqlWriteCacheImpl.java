package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Action;
import com.alvazan.orm.api.spi3.db.ByteArray;
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

	private static final Logger log = LoggerFactory.getLogger(NoSqlWriteCacheImpl.class);
	
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
		List<ByteArray> theKeys = new ArrayList<ByteArray>();
		for(byte[] k : keys) {
			theKeys.add(new ByteArray(k));
		}
		log.info("cf="+colFamily+" finding all keys="+theKeys);
		return rawSession.find(colFamily, keys);
	}
	
	public Row find(String colFamily, byte[] rowKey) {
		List<byte[]> rowKeys = new ArrayList<byte[]>();
		rowKeys.add(rowKey);
		log.info("cf="+colFamily+" finding the key="+new ByteArray(rowKey));
		List<Row> rows = rawSession.find(colFamily, rowKeys);
		return rows.get(0);
	}

	@Override
	public void flush() {
		logInformation();
		
		rawSession.sendChanges(actions, ormSession);
		actions = new ArrayList<Action>();
	}

	private void logInformation() {
		String msg = "Data being flushed to database=";
		for(Action act : actions) {
			String cf = act.getColFamily();
			msg += "\n"+cf;
			if(act instanceof Persist) {
				Persist p = (Persist) act;
				msg += " persist rowkey="+new ByteArray(p.getRowKey());
			} else if(act instanceof Remove) {
				Remove r = (Remove) act;
				msg += " remove  rowkey="+new ByteArray(r.getRowKey());
			} else if(act instanceof PersistIndex) {
				PersistIndex p = (PersistIndex) act;
				msg += " index persist="+new ByteArray(p.getRowKey())+" col="+
							new ByteArray(p.getColumn().getIndexedValue());
			} else if(act instanceof RemoveIndex) {
				RemoveIndex r = (RemoveIndex) act;
				msg += " index remove ="+new ByteArray(r.getRowKey())+" col="+
							new ByteArray(r.getColumn().getIndexedValue());
			}
		}
		
		log.info(msg);
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
		log.info("cf="+colFamily+" column range scan row="+new ByteArray(rowKey));
		return rawSession.columnRangeScan(colFamily, rowKey, from, to, batchSize);
	}

	@Override
	public void setOrmSessionForMeta(Object session) {
		this.ormSession = session;
	}

}
