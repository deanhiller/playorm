package com.alvazan.orm.layer3.spi.inmemory;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.layer2.nosql.Row;

public class Table {

	private Map<String, Row> keyToRow = new HashMap<String, Row>();
	
	public Row findOrCreateRow(String key) {
		Row row = keyToRow.get(key);
		if(row == null) {
			row = new Row();
			keyToRow.put(key, row);
		}
		return row;
	}

	public void removeRow(String rowKey) {
		keyToRow.remove(rowKey);
	}

	public Row getRow(String key) {
		return keyToRow.get(key);
	}

}
