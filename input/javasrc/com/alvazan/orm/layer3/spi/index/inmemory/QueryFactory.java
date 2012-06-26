package com.alvazan.orm.layer3.spi.index.inmemory;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;
import com.alvazan.orm.layer3.spi.index.SpiQueryInfo;

public class QueryFactory implements SpiIndexQueryFactory {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private SpiQueryInfo info;
	
	public void setup(SpiQueryInfo info) {
		this.info = info;
	}

	@Override
	public SpiIndexQuery createQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setInfo(info);
		return indexQuery;
	}

}
