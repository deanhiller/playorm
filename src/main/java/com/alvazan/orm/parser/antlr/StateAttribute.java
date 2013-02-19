package com.alvazan.orm.parser.antlr;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class StateAttribute {

	private DboColumnMeta columnInfo; 
	private ViewInfoImpl tableInfo;
	private String textInSql;
	
	public StateAttribute(ViewInfoImpl tableInfo, DboColumnMeta columnName2, String textInSql) {
		this.tableInfo = tableInfo;
		this.columnInfo = columnName2;
		this.textInSql = textInSql;
	}

	public String getTextInSql() {
		return textInSql;
	}


	public ViewInfoImpl getViewInfo() {
		return tableInfo;
	}

	public DboColumnMeta getColumnInfo() {
		return columnInfo;
	}
	
}
