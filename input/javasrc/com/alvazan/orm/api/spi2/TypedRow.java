package com.alvazan.orm.api.spi2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class TypedRow<T> {

	private T rowKey;
	private Map<String, TypedColumn> columns = new HashMap<String, TypedColumn>();
	
	public T getRowKey() {
		return rowKey;
	}
	public void setRowKey(T rowKey) {
		this.rowKey = rowKey;
	}
	public TypedColumn getColumn(String colName) {
		return columns.get(colName);
	}
	public void addColumn(TypedColumn col) {
		columns.put(col.getName(), col);
	}
	
	public Collection<TypedColumn> getColumnsAsColl() {
		return columns.values();
	}
	
}
