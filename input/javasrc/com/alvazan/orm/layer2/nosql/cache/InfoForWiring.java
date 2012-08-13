package com.alvazan.orm.layer2.nosql.cache;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi2.meta.DboTableMeta;
import com.alvazan.orm.layer2.indexing.ExpressionNode;

public class InfoForWiring {

	private DboTableMeta noAliasTable;
	private Map<String, DboTableMeta> aliasToMeta = new HashMap<String, DboTableMeta>();
	private boolean selectStarDefined;
	private String query;
	private String targetTable;
	private ExpressionNode astTree;
	
	public InfoForWiring(String query, String targetTable) {
		this.query = query;
		this.targetTable= targetTable;
	}

	public void setNoAliasTable(DboTableMeta metaClass) {
		this.noAliasTable = metaClass;
	}

	public DboTableMeta getNoAliasTable() {
		return noAliasTable;
	}

	public void put(String alias, DboTableMeta metaClass) {
		aliasToMeta.put(alias, metaClass);
	}

	public DboTableMeta getInfoFromAlias(String alias) {
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
	
	

}
