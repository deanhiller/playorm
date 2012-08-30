package com.alvazan.orm.layer5.indexing;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.layer5.nosql.cache.PartitionMeta;
import com.alvazan.orm.parser.antlr.JoinInfo;

/**
 * A Table can have different aliases like in a self-join where the table is alias "a" and alias "b"
 * This class is a View of a or a View of b, such that everyone referring to "a" has one ViewInfo and
 * everyone with "b" has nother ViewInfo 
 * @author dhiller2
 */
public class ViewInfo {

	private String alias;
	private DboTableMeta tableMeta;
	private List<JoinInfo> joins = new ArrayList<JoinInfo>();
	private PartitionMeta partition;
	
	public ViewInfo(String alias, DboTableMeta tableMeta) {
		this.alias = alias;
		this.tableMeta = tableMeta;
	}
	
	@Override
	public String toString() {
		return "ViewInfo [alias=" + alias + ", table="+tableMeta.getColumnFamily()+"]";
	}

	public DboTableMeta getTableMeta() {
		return tableMeta;
	}
	public List<JoinInfo> getJoins() {
		return joins;
	}
	public void setPartition(PartitionMeta p) {
		this.partition = p;
	}
	public PartitionMeta getPartition() {
		return partition;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public void addJoin(JoinInfo join) {
		joins.add(join);
	}
	public JoinInfo getJoinInfo(ViewInfo table2) {
		//even self joins will have a different alias
		if(table2.equals(this))
			throw new IllegalArgumentException("bug, you can't pass in our view even on self join because view is always different");
		for(JoinInfo info : joins) {
			if(info.hasView(table2))
				return info;
		}
		return null;
	}

	public JoinInfo findViewMatch(ViewInfo infoR) {
		if(this == infoR) {
			JoinInfo info = new JoinInfo(this, null, null, null, JoinType.NONE);
			return info;
		}
		return null;
	}

}
