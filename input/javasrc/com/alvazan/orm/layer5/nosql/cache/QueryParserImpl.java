package com.alvazan.orm.layer5.nosql.cache;

import javax.inject.Inject;
import javax.inject.Named;

import com.alvazan.orm.api.spi3.meta.MetaAndIndexTuple;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi3.meta.QueryParser;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;

public class QueryParserImpl implements QueryParser {

	@Inject
	@Named("logger")
	private NoSqlRawSession rawSession;
	@Inject
	private ScannerForQuery scanner;
	
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
	public MetaQuery parseQueryForOrm(String query, String targetTable, String errorMsg) {
		return scanner.newsetupByVisitingTree(query, targetTable, null, errorMsg);
	}

	@Override
	public void close() {
		rawSession.close();
	}

}
