package com.alvazan.orm.layer3.spi.index.inmemory;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;

public class QueryFactory<T> implements SpiIndexQueryFactory<T> {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private MetaQuery<T> info;
	private MetaClass<T> clazz;
	private Indice indice;
	
	public void init(Indice indice) {
		this.indice = indice;
	}
	
	public void setup(MetaClass<T> clazz, MetaQuery<T> info) {
		this.info = info;
		this.clazz = clazz;
	}

	@Override
	public SpiIndexQuery<T> createQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl<T> indexQuery = factory.get();
		RAMDirectory ramDir = indice.findOrCreate(indexName);
		indexQuery.setup(clazz, info, ramDir, indexName);
		return indexQuery;
	}

}
