package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi3.db.Action;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Persist;
import com.alvazan.orm.api.spi3.db.Remove;
import com.alvazan.orm.api.spi3.db.Row;

public class InMemorySession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(InMemorySession.class);
	
	@Inject
	private NoSqlDatabase database;
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	@Override
	public List<Row> find(String colFamily, List<byte[]> keys) {
		List<Row> rows = new ArrayList<Row>();
		for(byte[] key : keys) {
			Row row = findRow(colFamily, key);
			//This add null if there is no row to the list on purpose
			rows.add(row);
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
		for(Action action : actions) {
			
			Table table = lookupColFamily(action, (NoSqlEntityManager) ormSession);
			
			if(action instanceof Persist) {
				persist((Persist)action, table);
			} else if(action instanceof Remove) {
				remove((Remove)action, table);
			}
		}
	}

	@SuppressWarnings("rawtypes")
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
			DboDatabaseMeta db = mgr.find(DboDatabaseMeta.class, NoSqlEntityManager.META_DB_KEY);
			cf = db.getMeta(colFamily);
		}
			
		Class columnNameType = cf.getColumnNameType();

		SortType sortType = SortType.BYTES;
		if(String.class.equals(columnNameType))
			sortType = SortType.UTF8;
		else if(Integer.class.equals(columnNameType)
				|| Long.class.equals(columnNameType)
				|| Short.class.equals(columnNameType)
				|| Byte.class.equals(columnNameType))
			sortType = SortType.INTEGER;
		else if(Float.class.equals(columnNameType)
				|| Double.class.equals(columnNameType))
			sortType = SortType.DECIMAL;
			
		table = new Table(sortType);
		database.putTable(colFamily, table);
		
		return table;
	}

	private void remove(Remove action, Table table) {
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
	public void clearDatabaseIfInMemoryType() {
		database.clear();
	}

	@Override
	public void start(Map<String, String> properties) {
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<Column> columnRangeScan(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) {
		Table table = database.findTable(colFamily);
		if(table == null) {
			return new HashSet<Column>();
		}
		Row row = table.findOrCreateRow(rowKey);
		if(row == null)
			return new HashSet<Column>();

		return row.columnSlice(from, to);
	}
}
