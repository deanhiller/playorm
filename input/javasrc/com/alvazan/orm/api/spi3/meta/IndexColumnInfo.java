package com.alvazan.orm.api.spi3.meta;

import com.alvazan.orm.api.spi9.db.IndexColumn;

public class IndexColumnInfo {

	private IndexColumn primary;
	private IndexColumnInfo andedColumns;
	private DboColumnMeta columnMeta;
	
	public IndexColumn getPrimary() {
		return primary;
	}
	public void setPrimary(IndexColumn primary) {
		this.primary = primary;
	}

	public DboColumnMeta getColumnMeta() {
		return columnMeta;
	}
	public void setColumnMeta(DboColumnMeta columnMeta) {
		this.columnMeta = columnMeta;
	}
	
}
