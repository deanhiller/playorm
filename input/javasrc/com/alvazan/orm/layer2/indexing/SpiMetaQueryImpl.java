package com.alvazan.orm.layer2.indexing;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.SpiMetaQuery;
import com.alvazan.orm.api.spi2.SpiQueryAdapter;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(SpiMetaQueryImpl.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String partitionBy, String partitionId, NoSqlSession session) {
		log.info("creating query for partition=/"+partitionBy+"/"+partitionId);
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(partitionBy, partitionId, this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node) {
		this.astTreeRoot = node;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	
	
}
