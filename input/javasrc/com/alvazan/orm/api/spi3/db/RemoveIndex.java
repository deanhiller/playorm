package com.alvazan.orm.api.spi3.db;

import java.util.ArrayList;
import java.util.List;


public class RemoveIndex implements Action {
	private String colFamily;
	private byte[] rowKey;
	private IndexColumn column;
	
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
	
}
