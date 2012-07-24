package com.alvazan.orm.layer3.spi.index.solr;

import com.alvazan.orm.api.spi.index.ExpressionNode;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;

public class SolrMetaQueryImpl implements SpiMetaQuery {

	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName) {
		return null;
	}

	@Override
	public void setASTTree(ExpressionNode node) {
		
	}

}
