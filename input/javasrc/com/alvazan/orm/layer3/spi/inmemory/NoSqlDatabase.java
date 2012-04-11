package com.alvazan.orm.layer3.spi.inmemory;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

@Singleton
public class NoSqlDatabase {

	private Map<String, Table> nameToTable = new HashMap<String, Table>();
	
	public Table getTable(String name) {
		Table table = nameToTable.get(name);
		if(table == null) {
			table = new Table();
			nameToTable.put(name, table);
		}
		return table;
	}
}
