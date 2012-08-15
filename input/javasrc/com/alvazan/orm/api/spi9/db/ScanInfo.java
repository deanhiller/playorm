package com.alvazan.orm.api.spi9.db;

public class ScanInfo {
	private String indexColFamily; 
	private byte[] rowKey;
	private String entityColFamily;
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
	
}
