package com.alvazan.orm.impl;

import java.util.List;

import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.NoSqlEntityManager;

public class NoSqlEntityManagerImpl implements NoSqlEntityManager {

	@Override
	public void put(Object entity) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void putAll(List<Object> entities) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T> List<T> findAll(Class<T> entityType, List<Object> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T> Index<T> getIndex(Class<T> forEntity, String indexName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T find(Class<T> entityType, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getReference(Class<T> entityType, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

}
