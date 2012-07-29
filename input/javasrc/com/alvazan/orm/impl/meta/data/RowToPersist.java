package com.alvazan.orm.impl.meta.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.Column;

public class RowToPersist {
	private byte[] key;
	private List<Column> columns = new ArrayList<Column>();
	private SortedMap<String, Column> lazySortedColumns;
	
	private Set<ByteArray> columnsToRemove = new HashSet<ByteArray>();
	
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public boolean hasRemoves() {
		return columnsToRemove.size() > 0;
	}
	public List<byte[]> getColumnNamesToRemove() {
		List<byte[]> coll = new ArrayList<byte[]>();
		for(ByteArray b : columnsToRemove) {
			coll.add(b.getKey());
		}
		return coll;
	}
	
}
