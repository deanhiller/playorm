package com.alvazan.orm.api.spi1.meta;

import java.io.UnsupportedEncodingException;

import com.alvazan.orm.api.spi3.db.IndexColumn;

public class IndexData {
	private String columnFamilyName;
	private String rowKey;
	private IndexColumn indexColumn = new IndexColumn();
	
	public void setColumnFamilyName(String indexTableName) {
		this.columnFamilyName = indexTableName;
	}

	public void setRowKey(String key) {
		this.rowKey = key;
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

	public IndexColumn getIndexColumn() {
		return indexColumn;
	}

}
