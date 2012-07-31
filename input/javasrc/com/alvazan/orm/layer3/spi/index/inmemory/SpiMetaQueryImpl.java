package com.alvazan.orm.layer3.spi.index.inmemory;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.index.ExpressionNode;
import com.alvazan.orm.api.spi3.index.SpiMetaQuery;
import com.alvazan.orm.api.spi3.index.SpiQueryAdapter;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(SpiMetaQueryImpl.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName, NoSqlSession session) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(indexName, this, session);
		return indexQuery;
	}

	@Override
	public void setASTTree(ExpressionNode node) {
		this.astTreeRoot = node;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	
	
}
