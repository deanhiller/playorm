package com.alvazan.orm.api.spi3.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;


public class Row {
	private byte[] key;
	private NavigableMap<ByteArray, Column> columns = new TreeMap<ByteArray, Column>();
	
	public Row() {
	}

	public Row(TreeMap<ByteArray, Column> map) {
		this.columns = map;
	}
	
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	
	public Collection<Column> getColumns() {
		return columns.values();
	}

	public Column getColumn(byte[] key) {
		return columns.get(new ByteArray(key));
	}
	public void put(byte[] name, Column col) {
		ByteArray key = new ByteArray(name);
		columns.put(key, col);
	}
	public void remove(byte[] name) {
		ByteArray key = new ByteArray(name);
		columns.remove(key);
	}
	
	public Collection<Column> columnSlice(byte[] from, byte[] to) {
		ByteArray fromArray = new ByteArray(from);
		ByteArray toArray = new ByteArray(to);
		SortedMap<ByteArray, Column> result = columns.subMap(fromArray, true, toArray, true);
		return result.values();
	}

	public Collection<Column> columnByPrefix(byte[] prefix) {
		List<Column> prefixed = new ArrayList<Column>();
		boolean started = false;
		for(Entry<ByteArray, Column> col : columns.entrySet()) {
			if(col.getKey().hasPrefix(prefix)) {
				started = true;
				prefixed.add(col.getValue());
			} else if(started)
				break; //since we hit the prefix and we are sorted, we can break.
		}
		return prefixed;
	}
	
	public SortedMap<ByteArray, Column> getSortedColumns() {
		return columns;
	}
}
