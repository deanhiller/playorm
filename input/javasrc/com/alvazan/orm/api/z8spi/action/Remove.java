package com.alvazan.orm.api.z8spi.action;

import java.util.Collection;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;


public class Remove implements Action {
	private DboTableMeta colFamily;
	private byte[] rowKey;
	private RemoveEnum action;
	private Collection<byte[]> columns;
	
	public RemoveEnum getAction() {
		return action;
	}
	public void setAction(RemoveEnum action) {
		this.action = action;
	}
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
	public Collection<byte[]> getColumns() {
		return columns;
	}
	public void setColumns(Collection<byte[]> columnNames) {
		this.columns = columnNames;
	}
}
