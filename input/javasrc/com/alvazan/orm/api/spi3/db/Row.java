package com.alvazan.orm.api.spi3.db;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;


public class Row {
	private byte[] key;
	private SortedMap<ByteArray, Column> columns = new TreeMap<ByteArray, Column>();
	
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

		SortedMap<ByteArray, Column> tailMap = columns.tailMap(fromArray);
		SortedMap<ByteArray, Column> result = tailMap.headMap(toArray);
		
		return result.values();
	}

}
