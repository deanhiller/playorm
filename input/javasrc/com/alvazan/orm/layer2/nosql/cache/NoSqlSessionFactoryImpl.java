package com.alvazan.orm.layer2.nosql.cache;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.spi1.meta.MetaAndIndexTuple;
import com.alvazan.orm.api.spi1.meta.MetaQuery;
import com.alvazan.orm.api.spi1.meta.NoSqlSessionFactory;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.SpiQueryAdapter;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Row;

public class NoSqlSessionFactoryImpl implements NoSqlSessionFactory {

	@Inject
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Row> runQuery(String query) {
		NoSqlSession session = createSession();
		
		MetaAndIndexTuple tuple = parseQueryForAdHoc(query);
		String indexName = tuple.getIndexName();
		MetaQuery metaQuery = tuple.getMetaQuery();
		SpiQueryAdapter spiQueryAdapter = metaQuery.createSpiMetaQuery(indexName, session);
		
		List<String> primaryKeys = spiQueryAdapter.getResultList();
		String colFamily = metaQuery.getTargetTable().getColumnFamily();
		
		List<byte[]> keys = new ArrayList<byte[]>();
		for(String key : primaryKeys) {
			keys.add(key.getBytes());
		}
		
		List<Row> rows = session.find(colFamily, keys);
		
		return rows;
	}
	
	@SuppressWarnings("rawtypes")
	public MetaAndIndexTuple parseQueryForAdHoc(String query) {		
		MetaQuery metaQuery = scanner.parseQuery(query);
		
		MetaAndIndexTuple tuple = new MetaAndIndexTuple();
		tuple.setMetaQuery(metaQuery);
		//tuple.setIndexName(split[1]);
		return tuple;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public MetaQuery parseQueryForOrm(String query, String targetTable) {
		return scanner.newsetupByVisitingTree(query, targetTable);
	}

	@Override
	public void close() {
		rawSession.close();
	}

}
