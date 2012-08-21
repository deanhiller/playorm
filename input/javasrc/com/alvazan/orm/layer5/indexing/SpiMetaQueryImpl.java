package com.alvazan.orm.layer5.indexing;

import java.util.List;

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
	private TableInfo mainTable;
	private List<TableInfo> tables;
	
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

	public void setASTTree(ExpressionNode node, TableInfo mainTable, List<TableInfo> tables) {
		this.astTreeRoot = node;
		this.mainTable = mainTable;
		this.tables = tables;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	public DboTableMeta getMainTableMeta() {
		return mainTable.getTableMeta();
	}
	
}
