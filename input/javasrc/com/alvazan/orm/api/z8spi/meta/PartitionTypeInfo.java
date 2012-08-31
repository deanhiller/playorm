package com.alvazan.orm.api.z8spi.meta;


public class PartitionTypeInfo {

	private String partitionBy;
	private String partitionId;
	private Object colMeta;
	
	public PartitionTypeInfo(String by, String id, Object colMeta) {
		partitionBy = by;
		partitionId = id;
		this.colMeta = colMeta;
	}
	public String getPartitionBy() {
		return partitionBy;
	}
	public String getPartitionId() {
		return partitionId;
	}
	public Object getColMeta() {
		return colMeta;
	}
	
}
