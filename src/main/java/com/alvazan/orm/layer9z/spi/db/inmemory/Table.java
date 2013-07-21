package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.layer9z.spi.db.inmemory.IndexedRow.OurKey;

public class Table {

	private Map<ByteArray, Row> keyToRow = new HashMap<ByteArray, Row>();
	private String columnFamilyName;
	private SortType columnSortType;
	static final Comparator<ByteArray> UTF_COMPARATOR = new Utf8Comparator();
	static final Comparator<ByteArray> INTEGER_COMPARATOR = new IntegerComparator();
	static final Comparator<ByteArray> DECIMAL_COMPARATOR = new DecimalComparator();
	private static Comparator<OurKey> utfPrefixComparator = new PrefixComparator(UTF_COMPARATOR);
	private static Comparator<OurKey> integerPrefixComparator = new PrefixComparator(INTEGER_COMPARATOR);
	private static Comparator<OurKey> decimalPrefixComparator = new PrefixComparator(DECIMAL_COMPARATOR);
	
	public Table(String columnFamily, SortType sortType) {
		this.columnSortType = sortType;
		this.columnFamilyName = columnFamily;
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
			row = new RowImpl(tree);
			break;
		case UTF8:
			tree = new TreeMap<ByteArray, Column>(UTF_COMPARATOR);
			row = new RowImpl(tree);
			break;
		case INTEGER:
			tree = new TreeMap<ByteArray, Column>(INTEGER_COMPARATOR);
			row = new RowImpl(tree);
			break;
		case DECIMAL:
			tree = new TreeMap<ByteArray, Column>(DECIMAL_COMPARATOR);
			row = new RowImpl(tree);
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

	@Override
	public String toString() {
		String t = "";
		t += "columnFamilyName="+columnFamilyName+" columnSortType="+columnSortType;
		for(Row r : keyToRow.values()) {
			ByteArray b = new ByteArray(r.getKey());
			t += "\nrowkey="+b.asString()+" row="+r;
		}
		return t;
	}

	public Set<ByteArray> findAllKeys() {
		return keyToRow.keySet();
	}
	
}
