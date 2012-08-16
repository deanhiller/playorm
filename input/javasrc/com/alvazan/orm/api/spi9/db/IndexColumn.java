package com.alvazan.orm.api.spi9.db;

public class IndexColumn {
	private byte[] indexedValue;
	private byte[] primaryKey;
	private Long timestamp;
	private String columnName;

	public IndexColumn() {}
	
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getIndexedValue() {
		return indexedValue;
	}

	public void setIndexedValue(byte[] indexedValue) {
		this.indexedValue = indexedValue;
	}

	public byte[] getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(byte[] primaryKey) {
		this.primaryKey = primaryKey;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

}
