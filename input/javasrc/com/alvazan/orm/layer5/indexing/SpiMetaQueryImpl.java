package com.alvazan.orm.layer5.indexing;

import java.util.List;

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
	private List<TableInfo> tables;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(NoSqlSession session) {
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node, TableInfo mainTable, List<TableInfo> tables) {
		this.astTreeRoot = node;
		this.mainTable = mainTable;
		this.tables = tables;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	public TableInfo getMainTableMeta() {
		return mainTable;
	}
	
}
