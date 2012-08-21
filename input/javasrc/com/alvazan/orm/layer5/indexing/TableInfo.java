package com.alvazan.orm.layer5.indexing;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.DboTableMeta;

public class TableInfo {

	private JoinType joinType;
	private DboTableMeta tableMeta;
	private Map<String, TableInfo> joinsByColumn = new HashMap<String, TableInfo>();
	
	public TableInfo(DboTableMeta tableMeta, JoinType joinType) {
		this.tableMeta = tableMeta;
		this.joinType = joinType;
	}
	public JoinType getJoinType() {
		return joinType;
	}
	public DboTableMeta getTableMeta() {
		return tableMeta;
	}
	public Map<String, TableInfo> getJoins() {
		return joinsByColumn;
	}
	public void putJoinTable(String column, TableInfo fkTableMeta) {
		joinsByColumn.put(column, fkTableMeta);
	}
	
	
}
