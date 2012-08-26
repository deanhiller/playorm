package com.alvazan.orm.layer5.indexing;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;

public class StateAttribute {

	private DboColumnMeta columnInfo; 
	private TableInfo tableInfo;
	private String textInSql;
	
	public StateAttribute(TableInfo tableInfo, DboColumnMeta columnName2, String textInSql) {
		this.tableInfo = tableInfo;
		this.columnInfo = columnName2;
		this.textInSql = textInSql;
	}

	public String getTextInSql() {
		return textInSql;
	}


	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public DboColumnMeta getColumnInfo() {
		return columnInfo;
	}
	
}
