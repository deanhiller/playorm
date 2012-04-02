package com.alvazan.nosql.spi;

import java.util.List;

public class Row {
	private String key;
	private List<Column> columns;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
}
