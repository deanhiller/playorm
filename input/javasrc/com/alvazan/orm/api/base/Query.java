package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.api.spi3.KeyValue;

public interface Query<T> {

	public void setParameter(String name, Object value);
	
	/**
	 * If there is an entity in your query result list where the index has the value and
	 * the nosql store has no entity, KeyValue contains the exception delaying
	 * the exception until code accesses that missing value.  In this method, if you only
	 * iterate through the first 4 elements and the missing element was #5, you will not see
	 * any exception at all and code will keep working.
	 * @return
	 */
	public List<KeyValue<T>> getResultKeyValueList();
	
	public T getSingleObject();
	
	/**
	 * You may want to use getResultKeyValueList instead since that will delay exceptions caused by info being in the
	 * index but the entity not existing.
	 * @return
	 */
	public List<T> getResultList();
	
}
