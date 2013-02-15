package com.alvazan.orm.api.z8spi.meta;

import java.util.List;
import java.util.Map;



public class InfoForIndex<T> {
	
	private T entity;
	private RowToPersist row;
	private String virtualCf;
	@SuppressWarnings("rawtypes")
	private Map fieldToValue;
	private List<PartitionTypeInfo> partitions;

	@SuppressWarnings("rawtypes")
	public InfoForIndex(T entity2, RowToPersist row2, String virtualCf,
			Map fieldToValue2, List<PartitionTypeInfo> partitions) {
		this.entity = entity2;
		this.row = row2;
		this.virtualCf = virtualCf;
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
		return virtualCf;
	}
	public void setColumnFamily(String columnFamily) {
		this.virtualCf = columnFamily;
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
