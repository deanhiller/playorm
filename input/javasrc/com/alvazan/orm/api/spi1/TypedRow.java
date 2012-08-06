package com.alvazan.orm.api.spi1;

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
	public Map<String, TypedColumn> getColumns() {
		return columns;
	}
	public void setColumns(Map<String, TypedColumn> columns) {
		this.columns = columns;
	}
	public void addColumn(TypedColumn col) {
		columns.put(col.getName(), col);
	}
	
	public Collection<TypedColumn> getColumnsAsColl() {
		return columns.values();
	}

}
