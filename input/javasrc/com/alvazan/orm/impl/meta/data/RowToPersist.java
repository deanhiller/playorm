package com.alvazan.orm.impl.meta.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.spi3.db.Column;

public class RowToPersist {
	private byte[] key;
	private List<Column> columns = new ArrayList<Column>();
	
	private Set<byte[]> columnsToRemove = new HashSet<byte[]>();
	
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
	public Set<byte[]> getColumnNamesToRemove() {
		return columnsToRemove;
	}
	public void addEntityToRemove(byte[] colName) {
		columnsToRemove.add(colName);
	}
	
}
