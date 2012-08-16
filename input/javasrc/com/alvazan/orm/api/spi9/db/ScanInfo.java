package com.alvazan.orm.api.spi9.db;

public class ScanInfo {
	private String indexColFamily; 
	private byte[] rowKey;
	//optional but logging won't work without it
	private String entityColFamily;
	//optional but logging won't work without it
	private String columnName;
	private int batchSize;

	public ScanInfo(String indexColFamily, byte[] rowKey2, int batchSize) {
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
		this.batchSize = batchSize;
	}
	
	public ScanInfo(String realColFamily, String colName, String indexColFamily,
			byte[] rowKey2, int batchSize) {
		this.entityColFamily = realColFamily;
		this.columnName = colName;
		this.indexColFamily = indexColFamily;
		this.rowKey = rowKey2;
		this.batchSize = batchSize;
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

	public int getBatchSize() {
		return batchSize;
	}
}
