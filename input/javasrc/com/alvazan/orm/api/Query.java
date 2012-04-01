package com.alvazan.orm.api;

import java.util.List;

public interface Query<T> {

	public void setParameter(String name, Object value);
	
	public T getSingleObject();
	
	public List<T> getResultList();
	
}
