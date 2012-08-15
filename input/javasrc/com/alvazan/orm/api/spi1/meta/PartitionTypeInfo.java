package com.alvazan.orm.api.spi1.meta;

public class PartitionTypeInfo {

	private String partitionBy;
	private String partitionId;
	
	public PartitionTypeInfo(String by, String id) {
		partitionBy = by;
		partitionId = id;
	}
	public String getPartitionBy() {
		return partitionBy;
	}
	public String getPartitionId() {
		return partitionId;
	}
	
}
