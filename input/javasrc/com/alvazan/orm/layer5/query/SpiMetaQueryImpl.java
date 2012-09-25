package com.alvazan.orm.layer5.query;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.parser.antlr.ExpressionNode;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	private Map<String, TypeInfo> parameterFieldMap;
	private String query;
	private List<ViewInfo> views;
	private List<ViewInfo> viewsEagerJoin;
	private List<ViewInfo> viewsDelayedJoin;
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(NoSqlSession session) {
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(this, session);
		return indexQuery;
	}

	public void setASTTree(ExpressionNode node, List<ViewInfo> views, List<ViewInfo> viewsEagerJoin, List<ViewInfo> viewsDelayedJoin) {
		this.astTreeRoot = node;
		this.views = views;
		this.viewsEagerJoin = viewsEagerJoin;
		this.viewsDelayedJoin = viewsDelayedJoin;
	}

	public ExpressionNode getASTTree() {
		return astTreeRoot;
	}

	
	public void setParameterFieldMap(Map<String, TypeInfo> parameterFieldMap) {
		this.parameterFieldMap = parameterFieldMap;
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

	@Override
	public List<ViewInfo> getTargetViews() {
		return views;
	}

	@Override
	public List<ViewInfo> getViewsDelayedJoin() {
		return viewsDelayedJoin;

	}

	@Override
	public List<ViewInfo> getViewsEagerJoin() {
		return viewsEagerJoin;
	}
	
}
