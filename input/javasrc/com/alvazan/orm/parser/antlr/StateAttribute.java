package com.alvazan.orm.parser.antlr;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class StateAttribute {

	private DboColumnMeta columnInfo; 
	private ViewInfo tableInfo;
	private String textInSql;
	
	public StateAttribute(ViewInfo tableInfo, DboColumnMeta columnName2, String textInSql) {
		this.tableInfo = tableInfo;
		this.columnInfo = columnName2;
		this.textInSql = textInSql;
	}

	public String getTextInSql() {
		return textInSql;
	}


	public ViewInfo getViewInfo() {
		return tableInfo;
	}

	public DboColumnMeta getColumnInfo() {
		return columnInfo;
	}
	
}
