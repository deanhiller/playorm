package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Action;
import com.alvazan.orm.layer3.spi.Column;
import com.alvazan.orm.layer3.spi.NoSqlRawSession;
import com.alvazan.orm.layer3.spi.Persist;
import com.alvazan.orm.layer3.spi.Remove;

public class NoSqlWriteCacheImpl implements NoSqlSession {

	@Inject
	private NoSqlRawSession rawSession;
	private List<Action> actions = new ArrayList<Action>();
	
	@Override
	public void persist(String colFamily, String rowKey, List<Column> columns) {
		Persist persist = new Persist();
		persist.setColFamily(colFamily);
		persist.setRowKey(rowKey);
		persist.setColumns(columns);
		actions.add(persist);
	}

	@Override
	public void remove(String colFamily, String rowKey) {
		Remove remove = new Remove();
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		actions.add(remove);			
	}
	
	@Override
	public void remove(String colFamily, String rowKey, List<String> columnNames) {
		Remove remove = new Remove();
		remove.setColFamily(colFamily);
		remove.setRowKey(rowKey);
		remove.setColumns(columnNames);
		actions.add(remove);		
	}

	@Override
	public List<Row> find(String colFamily, List<String> keys) {
		return rawSession.find(colFamily, keys);
	}

	@Override
	public void flush() {
		rawSession.sendChanges(actions);		
	}
}
