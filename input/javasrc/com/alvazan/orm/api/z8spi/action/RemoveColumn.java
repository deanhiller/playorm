package com.alvazan.orm.api.z8spi.action;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class RemoveColumn implements Action {
	private DboTableMeta colFamily;
	private byte[] rowKey;
	private byte[] column;

	public DboTableMeta getColFamily() {
		return colFamily;
	}

	public void setColFamily(DboTableMeta colFamily2) {
		this.colFamily = colFamily2;
	}

	public byte[] getRowKey() {
		return rowKey;
	}

	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public byte[] getColumn() {
		return column;
	}

	public void setColumn(byte[] columnName) {
		this.column = columnName;
	}
}
