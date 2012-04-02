package com.alvazan.orm.impl.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.nosql.spi.NoSqlSession;
import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.KeyValue;
import com.alvazan.orm.api.NoSqlEntityManager;

public class BaseEntityManagerImpl implements NoSqlEntityManager {

	@Inject 
	private NoSqlSession session;
	
	@Override
	public void put(Object entity) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		putAll(entities);
	}

	@Override
	public void putAll(List<Object> entities) {
		
	}

	@Override
	public <T> T find(Class<T> entityType, Object key) {
		List<Object> keys = new ArrayList<Object>();
		List<KeyValue<T>> entities = findAll(entityType, keys);
		return entities.get(0).getValue();
	}
	
	@Override
	public <T> List<KeyValue<T>> findAll(Class<T> entityType, List<Object> keys) {
		
		return null;
	}
	
	@Override
	public void flush() {
		//No-op, the base layer is immediate with no caching writes to be written all at once
	}

	@Override
	public <T> Index<T> getIndex(Class<T> forEntity, String indexName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getReference(Class<T> entityType, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

}
