package com.alvazan.orm.api.spi9.db;



public class RemoveIndex implements Action {
	private String colFamily;
	private byte[] rowKey;
	private IndexColumn column;
	private String realColFamily;
	
	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}
	public IndexColumn getColumn() {
		return column;
	}
	public void setColumn(IndexColumn column) {
		this.column = column;
	}
	public String getRealColFamily() {
		return realColFamily;
	}
	public void setRealColFamily(String realColFamily) {
		this.realColFamily = realColFamily;
	}
	
}
