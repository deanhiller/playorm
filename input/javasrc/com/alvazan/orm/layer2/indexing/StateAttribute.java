package com.alvazan.orm.layer2.indexing;

import com.alvazan.orm.api.spi1.meta.DboColumnMeta;

public class StateAttribute {

	private DboColumnMeta columnInfo; 
	private String tableName;
	
	public StateAttribute(String tableName2, DboColumnMeta columnName2) {
		this.tableName = tableName2;
		this.columnInfo = columnName2;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DboColumnMeta getColumnInfo() {
		return columnInfo;
	}

	public void setColumnInfo(DboColumnMeta columnName) {
		this.columnInfo = columnName;
	}	
	
	
}
