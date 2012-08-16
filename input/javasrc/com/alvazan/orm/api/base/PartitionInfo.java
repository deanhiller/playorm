package com.alvazan.orm.api.base;

public class PartitionInfo {
	private Class<?> joinClassType;
	private String partitionField;
	private Object partitionObj;
	
	public Class<?> getJoinClassType() {
		return joinClassType;
	}
	public void setJoinClassType(Class<?> joinClassType) {
		this.joinClassType = joinClassType;
	}
	public String getPartitionField() {
		return partitionField;
	}
	public void setPartitionField(String partitionField) {
		this.partitionField = partitionField;
	}
	public Object getPartitionObj() {
		return partitionObj;
	}
	public void setPartitionObj(Object partitionObj) {
		this.partitionObj = partitionObj;
	}

}
