package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi9.db.Action;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Persist;
import com.alvazan.orm.api.spi9.db.PersistIndex;
import com.alvazan.orm.api.spi9.db.Remove;
import com.alvazan.orm.api.spi9.db.RemoveIndex;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

public class InMemorySession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(InMemorySession.class);
	
	@Inject
	private NoSqlDatabase database;
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	@Override
	public Iterable<KeyValue<Row>> find(String colFamily, Iterable<byte[]> rowKeys) {
		List<KeyValue<Row>> rows = new ArrayList<KeyValue<Row>>();
		for(byte[] key : rowKeys) {
			Row row = findRow(colFamily, key);
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(key);
			kv.setValue(row);
			//This add null if there is no row to the list on purpose
			rows.add(kv);
		}
		
		return rows;
	}
	
	private Row findRow(String colFamily, byte[] key) {
		Table table = database.findTable(colFamily);
		if(table == null)
			return null;
		return table.getRow(key);
	}

	@Override
	public void sendChanges(List<Action> actions, Object ormSession) {
		sendChangesImpl(actions, ormSession);
	}
	
	public void sendChangesImpl(List<Action> actions, Object ormSession) {
		for(Action action : actions) {
			Table table = lookupColFamily(action, (NoSqlEntityManager) ormSession);
			if(action instanceof Persist) {
				persist((Persist)action, table);
			} else if(action instanceof Remove) {
				remove((Remove)action, table);
			} else if(action instanceof PersistIndex) {
				persistIndex((PersistIndex) action, table);
			} else if(action instanceof RemoveIndex) {
				removeIndex((RemoveIndex) action, table);
			}
		}
	}

	private void persistIndex(PersistIndex action, Table table) {
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		IndexedRow row = (IndexedRow) table.findOrCreateRow(rowKey);
		row.addIndexedColumn(column);
	}

	private void removeIndex(RemoveIndex action, Table table) {
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		IndexedRow row = (IndexedRow) table.findOrCreateRow(rowKey);
		row.removeIndexedColumn(column);		
	}
	
	private Table lookupColFamily(Action action, NoSqlEntityManager mgr) {
		String colFamily = action.getColFamily();
		Table table = database.findTable(colFamily);
		if(table != null)
			return table;
		
		log.info("CREATING column family="+colFamily+" in cassandra");
			
		DboTableMeta cf = dbMetaFromOrmOnly.getMeta(colFamily);
		if(cf == null) {
			//check the database now for the meta since it was not found in the ORM meta data.  This is for
			//those that are modifying meta data themselves
			//DboDatabaseMeta db = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
			cf = mgr.find(DboTableMeta.class, colFamily);
			log.info("cf from db="+cf);
			//cf = db.getMeta(colFamily);
		}
		
		if(cf == null) {
			throw new IllegalStateException("Column family='"+colFamily+"' was not found AND we looked up meta data for this column" +
					" family to create it AND we could not find that data so we can't create it for you");
		}

		SortType sortType;
		StorageTypeEnum prefixType = cf.getColNamePrefixType();
		if(prefixType == null) {
			switch (cf.getNameStorageType()) {
			case BYTES:
				sortType = SortType.BYTES;
				break;
			case DECIMAL:
				sortType = SortType.DECIMAL;
				break;
			case INTEGER:
				sortType = SortType.INTEGER;
				break;
			case STRING:
				sortType = SortType.UTF8;
				break;
			default:
				throw new UnsupportedOperationException("type not supported="+cf.getNameStorageType());
			}
		} else {
			switch(prefixType) {
			case DECIMAL:
				sortType = SortType.DECIMAL_PREFIX;
				break;
			case INTEGER:
				sortType = SortType.INTEGER_PREFIX;
				break;
			case STRING:
				sortType = SortType.UTF8_PREFIX;
				break;
			default:
				throw new UnsupportedOperationException("type not supported="+prefixType);
			}
		}
		
		table = new Table(colFamily, sortType);
		database.putTable(colFamily, table);
		
		return table;
	}

	private void remove(Remove action, Table table) {
		if(action.getAction() == null)
			throw new IllegalArgumentException("action param is missing ActionEnum so we know to remove entire row or just columns in the row");
		switch(action.getAction()) {
		case REMOVE_ENTIRE_ROW:
			table.removeRow(action.getRowKey());
			break;
		case REMOVE_COLUMNS_FROM_ROW:
			removeColumns(action, table);
			break;
		default:
			throw new RuntimeException("bug, unknown remove action="+action.getAction());
		}
	}

	private void removeColumns(Remove action, Table table) {
		Row row = table.getRow(action.getRowKey());
		if(row == null)
			return;
		
		for(byte[] name : action.getColumns()) {
			row.remove(name);
		}
	}

	private void persist(Persist action, Table table) {
		Row row = table.findOrCreateRow(action.getRowKey());
		
		for(Column col : action.getColumns()) {
			row.put(col.getName(), col);
		}
	}

	@Override
	public void clearDatabase() {
		database.clear();
	}

	@Override
	public void start(Map<String, Object> properties) {
		
	}

	@Override
	public Collection<Column> columnRangeScan(ScanInfo info, Key from, Key to, int batchSize) {
		String colFamily = info.getIndexColFamily();
		byte[] rowKey = info.getRowKey();
		Table table = database.findTable(colFamily);
		if(table == null) {
			return new HashSet<Column>();
		}
		Row row = table.findOrCreateRow(rowKey);
		if(row == null)
			return new HashSet<Column>();
		
		return row.columnSlice(from, to);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
