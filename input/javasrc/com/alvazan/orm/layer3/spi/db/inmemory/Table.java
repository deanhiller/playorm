package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.layer3.spi.db.inmemory.IndexedRow.OurKey;

public class Table {

	private Map<ByteArray, Row> keyToRow = new HashMap<ByteArray, Row>();
	private SortType columnSortType;
	static Comparator<ByteArray> utfComparator = new Utf8Comparator();
	static Comparator<ByteArray> integerComparator = new IntegerComparator();
	static Comparator<ByteArray> decimalComparator = new DecimalComparator();
	private static Comparator<OurKey> utfPrefixComparator = new PrefixComparator(utfComparator);
	private static Comparator<OurKey> integerPrefixComparator = new PrefixComparator(integerComparator);
	private static Comparator<OurKey> decimalPrefixComparator = new PrefixComparator(decimalComparator);
	
	public Table(SortType sortType) {
		this.columnSortType = sortType;
	}

	public Row findOrCreateRow(byte[] key) {
		ByteArray array = new ByteArray(key);
		Row row = keyToRow.get(array);
		if(row == null) {
			row = createSortedMap();
			row.setKey(key);
			keyToRow.put(new ByteArray(key), row);
		}
		return row;
	}

	private Row createSortedMap() {
		TreeMap<ByteArray, Column> tree;
		Row row;
		switch (columnSortType) {
		case BYTES:
			tree = new TreeMap<ByteArray, Column>();
			row = new Row(tree);
			break;
		case UTF8:
			tree = new TreeMap<ByteArray, Column>(utfComparator);
			row = new Row(tree);
			break;
		case INTEGER:
			tree = new TreeMap<ByteArray, Column>(integerComparator);
			row = new Row(tree);
			break;
		case DECIMAL:
			tree = new TreeMap<ByteArray, Column>(decimalComparator);
			row = new Row(tree);
			break;
		case DECIMAL_PREFIX:
			TreeMap<OurKey, IndexColumn> map = new TreeMap<OurKey, IndexColumn>(decimalPrefixComparator);
			row = new IndexedRow(map);
			break;
		case INTEGER_PREFIX:
			TreeMap<OurKey, IndexColumn> map2 = new TreeMap<OurKey, IndexColumn>(integerPrefixComparator);
			row = new IndexedRow(map2);
			break;
		case UTF8_PREFIX:
			TreeMap<OurKey, IndexColumn> map3 = new TreeMap<OurKey, IndexColumn>(utfPrefixComparator);
			row = new IndexedRow(map3);
			break;
		default:
			throw new UnsupportedOperationException("not supported type="+columnSortType);
		}
		
		return row;
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
