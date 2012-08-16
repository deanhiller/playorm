package com.alvazan.orm.api.spi3.meta;

import java.util.List;
import java.util.Map;


public class InfoForIndex<T> {
	
	private T entity;
	private RowToPersist row;
	private String columnFamily;
	@SuppressWarnings("rawtypes")
	private Map fieldToValue;
	private List<PartitionTypeInfo> partitions;

	@SuppressWarnings("rawtypes")
	public InfoForIndex(T entity2, RowToPersist row2, String columnFamily2,
			Map fieldToValue2, List<PartitionTypeInfo> partitions) {
		this.entity = entity2;
		this.row = row2;
		this.columnFamily = columnFamily2;
		this.fieldToValue = fieldToValue2;
		this.partitions = partitions;
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
	@SuppressWarnings("rawtypes")
	public Map getFieldToValue() {
		return fieldToValue;
	}
	@SuppressWarnings("rawtypes")
	public void setFieldToValue(Map fieldToValue) {
		this.fieldToValue = fieldToValue;
	}
	public List<PartitionTypeInfo> getPartitions() {
		return partitions;
	}
	
}
