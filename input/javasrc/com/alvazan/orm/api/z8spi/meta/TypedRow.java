package com.alvazan.orm.api.z8spi.meta;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;

public class TypedRow {

	private Object rowKey;
	private NavigableMap<ByteArray, TypedColumn> columns = new TreeMap<ByteArray, TypedColumn>();
	private DboTableMeta metaClass;
	private ViewInfo view;
	
	public TypedRow() {
	}
	
	public TypedRow(ViewInfo view, DboTableMeta metaClass) {
		this.view = view;
		this.metaClass = metaClass;
	}
	
	public TypedRow(int i) {}

	public void addColumn(DboColumnToManyMeta colMeta, byte[] fullName, byte[] namePrefix, byte[] fk, byte[] value, Long timestamp) {
		TypedColumn c = new TypedColumn(colMeta, namePrefix, fk, value, timestamp);
		ByteArray b = new ByteArray(fullName);
		columns.put(b, c);
	}

	public void addColumn(DboColumnEmbedSimpleMeta colMeta, byte[] fullName, byte[] namePrefix, byte[] fk, byte[] value, Long timestamp) {
		TypedColumn c = new TypedColumn(colMeta, namePrefix, fk, value, timestamp);
		ByteArray b = new ByteArray(fullName);
		columns.put(b, c);
	}

	public void addColumn(DboColumnToOneMeta colMeta, byte[] fullName, byte[] namePrefix, byte[] fk, byte[] value, Long timestamp) {
		TypedColumn c = new TypedColumn(colMeta, namePrefix, fk, value, timestamp);
		ByteArray b = new ByteArray(fullName);
		columns.put(b, c);
	}

	public void addColumn(DboColumnMeta colMeta,
			byte[] columnName, byte[] value, Long timestamp) {
		TypedColumn c = new TypedColumn(colMeta, columnName, value, timestamp);
		ByteArray b = new ByteArray(columnName);
		columns.put(b, c);
	}
	
	public void addColumn(byte[] name, byte[] value, Long timestamp) {
		ByteArray b = new ByteArray(name);
		TypedColumn c = new TypedColumn(null, name, value, timestamp);
		columns.put(b, c);
	}
	
	public void addColumn(String name, Object obj) {
		DboColumnMeta columnMeta = metaClass.getColumnMeta(name);
		byte[] nameBytes = StandardConverters.convertToBytes(name);
		ByteArray b = new ByteArray(nameBytes);
		if(columnMeta != null) {
			byte[] value = columnMeta.convertToStorage2(obj);
			TypedColumn c = new TypedColumn(columnMeta, nameBytes, value, null);
			columns.put(b, c);
			return;
		}
		
		byte[] value = StandardConverters.convertToBytes(obj);
		TypedColumn c = new TypedColumn(columnMeta, nameBytes, value, null);
		columns.put(b, c);		
	}
	
//	public void addColumn(String name, Object value) {
//		addColumn(name, value, null);
//	}
//	public void addColumn(DboColumnMeta meta, byte[] name, byte[] subName, byte[] value, Long timestamp) {
//		if(meta == null)
//			throw new IllegalArgumentException("must have meta");
//		TypedColumn col = new TypedColumn(meta, name, subName, value, timestamp);
//		String fullName = name+"."+subName;
//		
//		columns.put(fullName, col);
//	}
//
//	public void addColumn(String name, Object value, Long timestamp) {
//		DboColumnMeta colMeta = metaClass.getColumnMeta(name);
//		if(colMeta == null && !(value instanceof byte[]))
//			throw new IllegalArgumentException("Column="+name+" not found on this table AND your value was not byte[].  Use byte[] or pass in a column we know about");
//		TypedColumn col = new TypedColumn(colMeta, name, value, timestamp);
//		columns.put(name, col);
//	}
//
//	public void addColumnString(String name, String value) {
//		DboColumnMeta colMeta = metaClass.getColumnMeta(name);
//		if(colMeta == null)
//			throw new IllegalArgumentException("Column="+name+" not found on this table.  Use addColumn passing in byte[] instead if you want to save a column the schema doesn't know about");
//		Object val = colMeta.convertStringToType(value);
//		TypedColumn col = new TypedColumn(colMeta, name, val, null);
//		columns.put(name, col);
//	}
	
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
	
	public TypedColumn getColumn(byte[] colName) {
		ByteArray b = new ByteArray(colName);
		return columns.get(b);
	}
	
	public TypedColumn getColumn(String colName) {
		byte[] nameBytes = StandardConverters.convertToBytes(colName);
		ByteArray b = new ByteArray(nameBytes);
		return columns.get(b);
	}
	
	public Collection<TypedColumn> getColumnsAsColl() {
		return columns.values();
	}

	public ViewInfo getView() {
		return view;
	}

	public void setMeta(DboTableMeta dboTableMeta) {
		this.metaClass = dboTableMeta;
	}

	public void setView(ViewInfo view2) {
		this.view = view2;
	}

}
