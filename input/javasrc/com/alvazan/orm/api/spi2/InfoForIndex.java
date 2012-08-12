package com.alvazan.orm.api.spi2;

import java.util.Map;


public class InfoForIndex<T> {
	
	private T entity;
	private RowToPersist row;
	private String columnFamily;
	private Map fieldToValue;

	public InfoForIndex(T entity2, RowToPersist row2, String columnFamily2,
			Map fieldToValue2) {
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
	public Map getFieldToValue() {
		return fieldToValue;
	}
	public void setFieldToValue(Map fieldToValue) {
		this.fieldToValue = fieldToValue;
	}
	
}
