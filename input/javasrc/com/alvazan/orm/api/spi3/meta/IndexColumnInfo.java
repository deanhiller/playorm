package com.alvazan.orm.api.spi3.meta;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi9.db.IndexColumn;

public class IndexColumnInfo {

	private IndexColumn primary;
	private IndexColumnInfo nextAndedColumn;
	private IndexColumnInfo nextOrColumn;
	private DboColumnMeta columnMeta;
	private transient ByteArray cachedPrimaryKey;
	
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
	public ByteArray getPrimaryKey() {
		if(cachedPrimaryKey == null) {
			cachedPrimaryKey = new ByteArray(primary.getPrimaryKey());
		}
		return cachedPrimaryKey;
	}
	public IndexColumnInfo getNextAndedColumn() {
		return nextAndedColumn;
	}
	public void setNextAndedColumn(IndexColumnInfo nextAndedColumn) {
		this.nextAndedColumn = nextAndedColumn;
	}
	public IndexColumnInfo getNextOrColumn() {
		return nextOrColumn;
	}
	public void setNextOrColumn(IndexColumnInfo nextOrColumn) {
		this.nextOrColumn = nextOrColumn;
	}
	
}
