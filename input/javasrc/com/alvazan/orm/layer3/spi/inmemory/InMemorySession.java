package com.alvazan.orm.layer3.spi.inmemory;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Action;
import com.alvazan.orm.layer3.spi.NoSqlRawSession;

public class InMemorySession implements NoSqlRawSession {

	@Inject
	private NoSqlDatabase database;
	
	@Override
	public List<Row> find(String colFamily, List<String> keys) {
		List<Row> rows = new ArrayList<Row>();
		for(String key : keys) {
			Row row = findRow(colFamily, key);
			rows.add(row);
		}
		return rows;
	}

	private Row findRow(String colFamily, String key) {
		Table table = database.getTable(colFamily);
		Row row = table.getRow(key);
		return row;
	}

	@Override
	public void sendChanges(List<Action> actions) {
		
	}


}
