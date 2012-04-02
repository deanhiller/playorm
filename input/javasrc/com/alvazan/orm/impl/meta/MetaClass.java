package com.alvazan.orm.impl.meta;

import com.alvazan.nosql.spi.Row;

public class MetaClass<T> {

	private Class<T> metaClass;
	private String columnFamily;

	public void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	public Class<T> getMetaClass() {
		return metaClass;
	}

	public Row translateToRow(Object entity) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String colFamily) {
		this.columnFamily = colFamily;
	}

	
}
