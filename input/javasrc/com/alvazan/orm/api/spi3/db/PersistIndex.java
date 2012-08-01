package com.alvazan.orm.api.spi3.db;

import java.util.ArrayList;
import java.util.List;


public class PersistIndex implements Action {
	private String colFamily;
	private byte[] rowKey;
	private long timestamp;
	private IndexColumn column;
	private ColumnType columnType = ColumnType.ANY_EXCEPT_COMPOSITE;
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
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
	public ColumnType getColumnType() {
		return columnType;
	}
	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}
	public IndexColumn getColumn() {
		return column;
	}
	public void setColumn(IndexColumn column) {
		this.column = column;
	}
	
}
