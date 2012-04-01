package com.alvazan.orm.impl;

import javax.inject.Inject;

import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.google.inject.Provider;

public class NoSqlEntityMgrFactory implements NoSqlEntityManagerFactory {

	@Inject
	private Provider<NoSqlEntityManager> entityMgrProvider;
	
	@Override
	public NoSqlEntityManager createEntityManager() {
		return entityMgrProvider.get();
	}

}
