package com.alvazan.orm.parser.antlr;

import java.util.HashSet;
import java.util.Set;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.layer5.indexing.JoinType;
import com.alvazan.orm.layer5.indexing.ViewInfo;

public class JoinInfo {

	private ViewInfo primaryTable;
	private DboColumnMeta primaryCol;
	private ViewInfo secondaryTable;
	private DboColumnMeta secondaryCol;
	private JoinType joinType;
	private Set<ViewInfo> viewInfos = new HashSet<ViewInfo>();
	
	public JoinInfo(ViewInfo tableInfo, DboColumnMeta columnMeta,
			ViewInfo existing, DboColumnMeta colMeta2, JoinType type) {
		this.primaryTable = tableInfo;
		this.primaryCol = columnMeta;
		this.secondaryTable = existing;
		this.secondaryCol = colMeta2;
		this.joinType = type;
		viewInfos.add(primaryTable);
		if(secondaryTable != null)
			viewInfos.add(secondaryTable);
	}
	
	@Override
	public String toString() {
		String str = "[alias1="+primaryTable.getAlias();
		if(secondaryTable == null)
			return str +"]";
		return str+",alias2="+secondaryTable.getAlias()+"]";
	}


	public JoinType getJoinType() {
		return joinType;
	}

	public void setJoinType(JoinType joinType) {
		this.joinType = joinType;
	}


	public ViewInfo getPrimaryTable() {
		return primaryTable;
	}

	public DboColumnMeta getPrimaryCol() {
		return primaryCol;
	}

	public ViewInfo getSecondaryTable() {
		return secondaryTable;
	}

	public DboColumnMeta getSecondaryCol() {
		return secondaryCol;
	}

//	public boolean hasAlias(String alias) {
//		if(primaryTable.getAlias().equals(alias))
//			return true;
//		else if(secondaryTable.getAlias().equals(alias))
//			return true;
//		return false;
//	}

	public boolean hasView(ViewInfo view) {
		if(primaryTable == view)
			return true;
		else if(secondaryTable == view)
			return true;
		return false;
	}
	
	public Set<ViewInfo> getViews() {
		return viewInfos;
	}

}
