package com.alvazan.orm.layer5.nosql.cache;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.spi3.meta.MetaAndIndexTuple;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi3.meta.NoSqlSessionFactory;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;

public class NoSqlSessionFactoryImpl implements NoSqlSessionFactory {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
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
	public MetaAndIndexTuple parseQueryForAdHoc(String query, Object mgr) {		
		MetaQuery metaQuery = scanner.parseQuery(query, mgr);
		
		MetaAndIndexTuple tuple = new MetaAndIndexTuple();
		tuple.setMetaQuery(metaQuery);
		//tuple.setIndexName(split[1]);
		return tuple;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public MetaQuery parseQueryForOrm(String query, String targetTable) {
		return scanner.newsetupByVisitingTree(query, targetTable, null);
	}

	@Override
	public void close() {
		rawSession.close();
	}

}
