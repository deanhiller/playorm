package com.alvazan.orm.api.z8spi.meta;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TypedRow {

	private Object rowKey;
	private Map<String, TypedColumn> columns = new HashMap<String, TypedColumn>();
	private DboTableMeta metaClass;
	
	public TypedRow() {
		
	}
	
	public TypedRow(DboTableMeta metaClass) {
		this.metaClass = metaClass;
	}
	
	public void addColumn(String name, Object value) {
		addColumn(name, value, null);
	}
	public void addColumn(String name, Object value, Long timestamp) {
		DboColumnMeta colMeta = metaClass.getColumnMeta(name);
		if(colMeta == null && !(value instanceof byte[]))
			throw new IllegalArgumentException("Column="+name+" not found on this table AND your value was not byte[].  Use byte[] or pass in a column we know about");
		TypedColumn col = new TypedColumn(colMeta, name, value, timestamp);
		columns.put(name, col);
	}
	
	public void addColumnString(String name, String value) {
		DboColumnMeta colMeta = metaClass.getColumnMeta(name);
		if(colMeta == null)
			throw new IllegalArgumentException("Column="+name+" not found on this table.  Use addColumn passing in byte[] instead if you want to save a column the schema doesn't know about");
		Object val = colMeta.convertStringToType(value);
		TypedColumn col = new TypedColumn(colMeta, name, val, null);
		columns.put(name, col);
	}
	
	public Object getRowKey() {
		return rowKey;
	}
	public void setRowKey(Object rowKey) {
		this.rowKey = rowKey;
	}
	public String getRowKeyString() {
		return metaClass.getIdColumnMeta().convertTypeToString(rowKey);
	}
	public void setRowKeyString(String key) {
		rowKey = metaClass.getIdColumnMeta().convertStringToType(key);
	}
	public TypedColumn getColumn(String colName) {
		return columns.get(colName);
	}
	//public void addColumn(TypedColumn col) {
	//	columns.put(col.getName(), col);
	//}
	
	public Collection<TypedColumn> getColumnsAsColl() {
		return columns.values();
	}

	public void setMeta(DboTableMeta dboTableMeta) {
		this.metaClass = dboTableMeta;
	}
	
}
