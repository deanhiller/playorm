package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.layer3.spi.db.Row;

public class Table {

	private Map<String, Row> keyToRow = new HashMap<String, Row>();
	
	public Row findOrCreateRow(byte[] key) {
		Row row = keyToRow.get(key);
		if(row == null) {
			row = new Row();
			String strValue = convert(key);
			row.setKey(key);
			keyToRow.put(strValue, row);
		}
		return row;
	}

	private String convert(byte[] key) {
		String s = new String(key);
		return s;
	}

	public void removeRow(byte[] rowKey) {
		String strValue = convert(rowKey);
		keyToRow.remove(strValue);
	}

	public Row getRow(byte[] rowKey) {
		String strValue = convert(rowKey);
		return keyToRow.get(strValue);
	}
}
