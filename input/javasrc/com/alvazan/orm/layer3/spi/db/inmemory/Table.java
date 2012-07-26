package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;

public class Table {

	private Map<ByteArray, Row> keyToRow = new HashMap<ByteArray, Row>();
	private SortType columnSortType;
	private static Comparator<? super ByteArray> utfComparator = new Utf8Comparator();
	private static Comparator<? super ByteArray> integerComparator = new IntegerComparator();
	private static Comparator<? super ByteArray> decimalComparator = new DecimalComparator();
	
	public Table(SortType sortType) {
		this.columnSortType = sortType;
	}

	public Row findOrCreateRow(byte[] key) {
		ByteArray array = new ByteArray(key);
		Row row = keyToRow.get(array);
		if(row == null) {
			TreeMap<ByteArray, Column> map = createSortedMap();
			row = new Row(map);
			row.setKey(key);
			keyToRow.put(new ByteArray(key), row);
		}
		return row;
	}

	private TreeMap<ByteArray, Column> createSortedMap() {
		switch (columnSortType) {
		case BYTES:
			return new TreeMap<ByteArray, Column>();
		case UTF8:
			return new TreeMap<ByteArray, Column>(utfComparator);
		case INTEGER:
			return new TreeMap<ByteArray, Column>(integerComparator);
		case DECIMAL:
			return new TreeMap<ByteArray, Column>(decimalComparator);
		default:
			break;
		}
		return null;
	}

	public void removeRow(byte[] rowKey) {
		ByteArray key = new ByteArray(rowKey);
		keyToRow.remove(key);
	}

	public Row getRow(byte[] rowKey) {
		ByteArray key = new ByteArray(rowKey);
		Row row = keyToRow.get(key);
		return row;
	}
}
