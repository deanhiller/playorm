package com.alvazan.orm.layer3.spi;

import java.util.List;


public class Remove implements Action {
	private String colFamily;
	private String rowKey;
	private RemoveEnum action;
	private List<String> columns;
	
	public RemoveEnum getAction() {
		return action;
	}
	public void setAction(RemoveEnum action) {
		this.action = action;
	}
	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columnNames) {
		this.columns = columnNames;
	}
}
