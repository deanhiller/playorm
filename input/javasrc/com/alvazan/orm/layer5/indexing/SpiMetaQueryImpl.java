package com.alvazan.orm.layer5.indexing;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiMetaQuery;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	private TableInfo mainTable;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(NoSqlSession session) {
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node, TableInfo mainTable) {
		this.astTreeRoot = node;
		this.mainTable = mainTable;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	public TableInfo getMainTableMeta() {
		return mainTable;
	}
	
}
