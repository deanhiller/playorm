package com.alvazan.orm.layer5.indexing;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;

public class StateAttribute {

	private DboColumnMeta columnInfo; 
	private TableInfo tableInfo;
	
	public StateAttribute(TableInfo tableInfo, DboColumnMeta columnName2) {
		this.tableInfo = tableInfo;
		this.columnInfo = columnName2;
	}

	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public DboColumnMeta getColumnInfo() {
		return columnInfo;
	}
	
}
