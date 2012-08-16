package com.alvazan.orm.api.base;

public interface Partition<T> {

	public Query<T> createNamedQuery(String name);
	
	public Query<T> createNamedQueryJoin(String name, PartitionInfo... info);
	
}
