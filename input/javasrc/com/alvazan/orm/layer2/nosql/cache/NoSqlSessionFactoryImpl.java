package com.alvazan.orm.layer2.nosql.cache;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.api.spi.layer2.NoSqlSessionFactory;

public class NoSqlSessionFactoryImpl implements NoSqlSessionFactory {

	@Inject
	@Named("writecachelayer")
	private Provider<NoSqlSession> provider;
	@Inject
	private ScannerForQuery scanner;
	
	@Override
	public NoSqlSession createSession() {
		return provider.get();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public MetaQuery parseQuery(String query) {
		return scanner.parseQuery(query);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public MetaQuery newsetupByVisitingTree(String query, String targetTable) {
		return scanner.newsetupByVisitingTree(query, targetTable);
	}

}
