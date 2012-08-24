package com.alvazan.orm.api.spi9.db;

import java.io.UnsupportedEncodingException;

public class ScanInfo {
	private String indexColFamily; 
	private byte[] rowKey;
	//optional but logging won't work without it
	private String entityColFamily;
	//optional but logging won't work without it
	private String columnName;

	public ScanInfo(String indexColFamily, byte[] rowKey2) {
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
	}
	
	public ScanInfo(String realColFamily, String colName, String indexColFamily,
			byte[] rowKey2) {
		this.entityColFamily = realColFamily;
		this.columnName = colName;
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
	}
	
	public String getIndexColFamily() {
		return indexColFamily;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public String getEntityColFamily() {
		return entityColFamily;
	}
	public String getColumnName() {
		return columnName;
	}

	@Override
	public String toString() {
		return "CF="+indexColFamily+"(for cf="+entityColFamily+") rowKey="+toUTF8(rowKey);
	}

	private String toUTF8(byte[] data) {
		try {
			return new String(data, "UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
