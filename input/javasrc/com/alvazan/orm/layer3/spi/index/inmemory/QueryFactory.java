package com.alvazan.orm.layer3.spi.index.inmemory;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;

public class QueryFactory<T> implements SpiIndexQueryFactory<T> {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private Indice indice;
	
	public void init(Indice indice) {
		this.indice = indice;
	}
	
	@Override
	public SpiIndexQuery<T> createQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl<T> indexQuery = factory.get();
		RAMDirectory ramDir = indice.find(indexName);
		indexQuery.setup(ramDir, indexName);
		return indexQuery;
	}

}
