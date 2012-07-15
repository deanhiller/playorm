package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.alvazan.orm.api.spi.db.Action;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.db.Persist;
import com.alvazan.orm.api.spi.db.Remove;
import com.alvazan.orm.api.spi.db.Row;

public class InMemorySession implements NoSqlRawSession {

	@Inject
	private NoSqlDatabase database;
	
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
	public void sendChanges(List<Action> actions) {
		for(Action action : actions) {
			Table table = database.findOrCreateTable(action.getColFamily());
			if(action instanceof Persist) {
				persist((Persist)action, table);
			} else if(action instanceof Remove) {
				remove((Remove)action, table);
			}
		}
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
		
		Map<String, Column> columns = row.getColumns();
		for(String name : action.getColumns()) {
			columns.remove(name);
		}
	}

	private void persist(Persist action, Table table) {
		Row row = table.findOrCreateRow(action.getRowKey());
		
		Map<String, Column> columns = row.getColumns();
		for(Column col : action.getColumns()) {
			columns.put(col.getName(), col);
		}
	}

	@Override
	public void clearDatabaseIfInMemoryType() {
		database.clear();
	}
}
