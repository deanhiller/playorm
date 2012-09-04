package com.alvazan.orm.layer5.query;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.ViewInfo;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	private ViewInfo mainTable;
	private Map<String, TypeInfo> parameterFieldMap;
	private DboTableMeta targetTable;
	private String query;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(NoSqlSession session) {
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node, ViewInfo mainTable) {
		this.astTreeRoot = node;
		this.mainTable = mainTable;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	@Override
	public ViewInfo getMainViewMeta() {
		return mainTable;
	}

	public void setParameterFieldMap(Map<String, TypeInfo> parameterFieldMap) {
		this.parameterFieldMap = parameterFieldMap;
	}

	public void setTargetTable(DboTableMeta table) {
		this.targetTable = table;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public TypeInfo getMetaFieldByParameter(String name) {
		return parameterFieldMap.get(name);
	}

	@Override
	public String getQuery() {
		return query;
	}

}
