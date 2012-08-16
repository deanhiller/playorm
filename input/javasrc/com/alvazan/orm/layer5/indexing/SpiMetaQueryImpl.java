package com.alvazan.orm.layer5.indexing;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiMetaQuery;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(SpiMetaQueryImpl.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	private DboTableMeta mainTable;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String partitionBy, String partitionId, NoSqlSession session) {
		String partition = "/";
		if(partitionBy != null) {
			partition += partitionBy;
			if(partitionId != null)
				partition += "/"+partitionId;
		}
		log.info("creating query for partition="+partition);
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(partitionBy, partitionId, this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node, DboTableMeta mainTable) {
		this.astTreeRoot = node;
		this.mainTable = mainTable;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	public DboTableMeta getMainTableMeta() {
		return mainTable;
	}
	
}
