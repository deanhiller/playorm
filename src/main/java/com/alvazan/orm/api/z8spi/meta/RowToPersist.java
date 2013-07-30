package com.alvazan.orm.api.z8spi.meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.z8spi.action.Column;

public class RowToPersist {
	private byte[] key;
	private List<Column> columns = new ArrayList<Column>();
	
	private Set<byte[]> columnsToRemove = new HashSet<byte[]>();

	private List<IndexData> indexToAdd = new ArrayList<IndexData>();
	private List<IndexData> indexToRemove = new ArrayList<IndexData>();
	private byte[] virtualKey;
	private int ttl;

	public byte[] getKey() {
		return key;
	}
	public void setKeys(byte[] key, byte[] virtualKey) {
		this.key = key;
		this.virtualKey = virtualKey;
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
	public void addIndexToPersist(IndexData data) {
		indexToAdd.add(data);
	}
	public void addIndexToRemove(IndexData data) {
		indexToRemove.add(data);
	}
	public List<IndexData> getIndexToAdd() {
		return indexToAdd;
	}
	public void setIndexToAdd(List<IndexData> indexToAdd) {
		this.indexToAdd = indexToAdd;
	}
	public List<IndexData> getIndexToRemove() {
		return indexToRemove;
	}
	public void setIndexToRemove(List<IndexData> indexToRemove) {
		this.indexToRemove = indexToRemove;
	}
	public byte[] getVirtualKey() {
		return virtualKey;
	}
	public int getTtl() {
		return ttl;
	}
	public void setTtl(int ttl) {
		this.ttl = ttl;
	}
}
