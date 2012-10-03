package com.alvazan.orm.api.z8spi.action;

import java.util.List;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;



public class Persist implements Action {
	private DboTableMeta colFamily;
	private byte[] rowKey;
	private long timestamp;
	private List<Column> columns;

	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public DboTableMeta getColFamily() {
		return colFamily;
	}
	public void setColFamily(DboTableMeta colFamily) {
		this.colFamily = colFamily;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
}
