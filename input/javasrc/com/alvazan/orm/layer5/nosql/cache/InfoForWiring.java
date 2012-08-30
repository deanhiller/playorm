package com.alvazan.orm.layer5.nosql.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.TypeInfo;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.indexing.ViewInfo;

public class InfoForWiring {

	private Map<String,TypeInfo> parameterFieldMap = new HashMap<String, TypeInfo>();
	private ViewInfo noAliasTable;
	private Map<String, ViewInfo> aliasToMeta = new HashMap<String, ViewInfo>();
	private boolean selectStarDefined;
	private String query;
	private String targetTable;
	private ExpressionNode astTree;
	private ViewInfo firstTable;
	private Map<String, Integer> attributeUsedCount = new HashMap<String, Integer>();
	private DboTableMeta metaQueryTargetTable;
	
	public InfoForWiring(String query, String targetTable) {
		this.query = query;
		this.targetTable= targetTable;
	}
	
	public void setNoAliasTable(ViewInfo metaClass) {
		this.noAliasTable = metaClass;
	}

	public ViewInfo getNoAliasTable() {
		return noAliasTable;
	}

	public void putAliasTable(String alias, ViewInfo metaClass) {
		aliasToMeta.put(alias, metaClass);
	}

	public ViewInfo getInfoFromAlias(String alias) {
		return aliasToMeta.get(alias);
	}

	public void setSelectStarDefined(boolean defined) {
		selectStarDefined = defined;
	}

	public boolean isSelectStarDefined() {
		return selectStarDefined;
	}

	public String getQuery() {
		return query;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setAstTree(ExpressionNode node) {
		this.astTree = node;
	}

	public ExpressionNode getAstTree() {
		return astTree;
	}

	public ViewInfo getFirstTable() {
		return firstTable;
	}
	public void setFirstTable(ViewInfo t) {
		this.firstTable = t;
	}

	public void incrementAttributesCount(String attributeName) {
		int count = 0;
		Integer counter = attributeUsedCount.get(attributeName);
		if(counter != null) {
			count = counter;
		}
		count++;
		attributeUsedCount.put(attributeName, count);
	}

	public Map<String, Integer> getAttributeUsedCount() {
		return attributeUsedCount;
	}

	public Collection<String> getAllAliases() {
		return this.aliasToMeta.keySet();
	}

	public void setMetaQueryTargetTable(DboTableMeta metaClass) {
		this.metaQueryTargetTable= metaClass;
	}

	public DboTableMeta getMetaQueryTargetTable() {
		return metaQueryTargetTable;
	}

	public Map<String,TypeInfo> getParameterFieldMap() {
		return this.parameterFieldMap;
	}
}
