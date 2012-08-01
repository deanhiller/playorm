package com.alvazan.orm.impl.meta.data;

import java.io.UnsupportedEncodingException;

import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.ColumnType;

public class IndexData {
	private String columnFamilyName;
	private String rowKey;
	private byte[] indexedValue;
	private byte[] primaryKey;
	private StorageTypeEnum indexedValueType;
	
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

	public void setIndexedValue(byte[] byteVal) {
		indexedValue = byteVal;
	}

	public byte[] getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(byte[] primaryKey) {
		this.primaryKey = primaryKey;
	}

	public byte[] getIndexedValue() {
		return indexedValue;
	}

	public void setIndexedValueType(StorageTypeEnum storageType) {
		this.indexedValueType = storageType;
	}

	public StorageTypeEnum getIndexedValueType() {
		return indexedValueType;
	}

}
