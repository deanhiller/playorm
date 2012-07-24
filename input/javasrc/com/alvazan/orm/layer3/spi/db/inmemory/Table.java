package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.db.ByteArray;

public class Table {

	private Map<ByteArray, Row> keyToRow = new HashMap<ByteArray, Row>();
	
	public Row findOrCreateRow(byte[] key) {
		Row row = keyToRow.get(new ByteArray(key));
		if(row == null) {
			row = new Row();
			row.setKey(key);
			keyToRow.put(new ByteArray(key), row);
		}
		return row;
	}

	public void removeRow(byte[] rowKey) {
		keyToRow.remove(new ByteArray(rowKey));
	}

	public Row getRow(byte[] rowKey) {
		Row row = keyToRow.get(new ByteArray(rowKey));
		return row;
	}
}
