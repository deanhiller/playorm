package com.alvazan.orm.layer2.nosql.cache;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.api.spi.layer2.NoSqlSessionFactory;

public class NoSqlSessionFactoryImpl implements NoSqlSessionFactory {

	@Inject
	@Named("writecachelayer")
	private Provider<NoSqlSession> provider;
	
	@Override
	public NoSqlSession createSession() {
		return provider.get();
	}

}
