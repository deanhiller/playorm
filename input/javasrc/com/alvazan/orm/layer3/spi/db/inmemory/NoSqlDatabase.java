package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

@Singleton
public class NoSqlDatabase {

	private Map<String, Table> nameToTable = new HashMap<String, Table>();
	
	public Table findTable(String name) {
		return nameToTable.get(name);
	}

	public void putTable(String name, Table table) {
		nameToTable.put(name, table);
	}
	
	public void clear() {
		nameToTable.clear();
	}
}
