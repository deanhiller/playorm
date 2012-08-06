package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.NoDiscriminatorColumn;

@NoDiscriminatorColumn(value="Common")
public class DboColumnCommonMeta extends DboAbstractColumnMeta {

	private String columnType;
	
	private String indexPrefix;

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public String getIndexPrefix() {
		return indexPrefix;
	}

	public void setIndexPrefix(String indexPrefix) {
		this.indexPrefix = indexPrefix;
	}
	
}
