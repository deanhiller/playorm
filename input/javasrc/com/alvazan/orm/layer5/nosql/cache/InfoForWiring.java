package com.alvazan.orm.layer5.nosql.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.indexing.TableInfo;

public class InfoForWiring {

	private TableInfo noAliasTable;
	private Map<String, TableInfo> aliasToMeta = new HashMap<String, TableInfo>();
	private boolean selectStarDefined;
	private String query;
	private String targetTable;
	private ExpressionNode astTree;
	private TableInfo firstTable;
	private Map<String, Integer> attributeUsedCount = new HashMap<String, Integer>();
	private NoSqlEntityManager mgr;
	private List<TableInfo> tables;
	
	public InfoForWiring(String query, String targetTable, NoSqlEntityManager mgr) {
		this.query = query;
		this.targetTable= targetTable;
		this.mgr = mgr;
	}
	
	public NoSqlEntityManager getMgr() {
		return mgr;
	}


	public void setNoAliasTable(TableInfo metaClass) {
		this.noAliasTable = metaClass;
	}

	public TableInfo getNoAliasTable() {
		return noAliasTable;
	}

	public void putAliasTable(String alias, TableInfo metaClass) {
		aliasToMeta.put(alias, metaClass);
	}
	public void addFirstLevelTable(TableInfo table) {
		tables.add(table);
	}

	public TableInfo getInfoFromAlias(String alias) {
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

	public TableInfo getFirstTable() {
		return firstTable;
	}
	public void setFirstTable(TableInfo t) {
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

	public List<TableInfo> getTables() {
		return tables;
	}
	
}
