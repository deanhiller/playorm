package com.alvazan.orm.api;

public interface Index<T> {

	public void addToIndex(T entity);
	
	public void removeFromIndex(T entity);
	
	public Query<T> getNamedQuery(String name);
	
}
