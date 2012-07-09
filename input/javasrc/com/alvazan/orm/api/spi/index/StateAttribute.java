package com.alvazan.orm.api.spi.index;

public class StateAttribute {

	private String tableName;
	private String columnName;
	
	public StateAttribute(String tableName2, String columnName2) {
		this.tableName = tableName2;
		this.columnName = columnName2;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}	
	
	
}
