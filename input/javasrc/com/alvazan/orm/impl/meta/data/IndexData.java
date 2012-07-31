package com.alvazan.orm.impl.meta.data;

import java.io.UnsupportedEncodingException;

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

	public byte[] getRowKeyBytes() {
		try {
			return rowKey.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	

	
}
