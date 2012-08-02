package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;

public class InfoForIndex<T> {
	
	private T entity;
	private RowToPersist row;
	private String columnFamily;
	private Map<Field, Object> fieldToValue;

	public InfoForIndex(T entity2, RowToPersist row2, String columnFamily2,
			Map<Field, Object> fieldToValue2) {
		this.entity = entity2;
		this.row = row2;
		this.columnFamily = columnFamily2;
		this.fieldToValue = fieldToValue2;
	}
	
	public T getEntity() {
		return entity;
	}
	public void setEntity(T entity) {
		this.entity = entity;
	}
	public RowToPersist getRow() {
		return row;
	}
	public void setRow(RowToPersist row) {
		this.row = row;
	}
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	public Map<Field, Object> getFieldToValue() {
		return fieldToValue;
	}
	public void setFieldToValue(Map<Field, Object> fieldToValue) {
		this.fieldToValue = fieldToValue;
	}
	
}
