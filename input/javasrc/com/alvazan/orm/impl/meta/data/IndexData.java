package com.alvazan.orm.impl.meta.data;

public class IndexData {
	private String columnFamilyName;
	private String rowKey;
	private byte[] indexColumnName;
	
	public void setColumnFamilyName(String indexTableName) {
		this.columnFamilyName = indexTableName;
	}

	public void setIndexColumnName(byte[] indexColName) {
		this.indexColumnName = indexColName;
	}

	public void setRowKey(String key) {
		this.rowKey = key;
	}

	public byte[] getIndexColumnName() {
		return indexColumnName;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public String getRowKey() {
		return rowKey;
	}
	
	

	
}
