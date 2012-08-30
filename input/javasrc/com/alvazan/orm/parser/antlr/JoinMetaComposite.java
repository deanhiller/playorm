package com.alvazan.orm.parser.antlr;

import java.util.HashSet;
import java.util.Set;

import com.alvazan.orm.layer5.indexing.JoinType;
import com.alvazan.orm.layer5.indexing.ViewInfo;

public class JoinMetaComposite extends JoinMeta {

	private Set<JoinInfo> infos = new HashSet<JoinInfo>();
	private Set<ViewInfo> views = new HashSet<ViewInfo>();
	private JoinType joinType;

	public JoinMetaComposite(Set<JoinInfo> infos) {
		this.infos = infos;
	}

	public JoinMetaComposite(JoinInfo info, JoinType type, Set<JoinInfo> set1, Set<JoinInfo> set2) {
		this(info, type);
		addInfos(set1);
		addInfos(set2);
	}
	
	public JoinMetaComposite(JoinInfo info, JoinType type) {
		this.infos.add(info);
		views.addAll(info.getViews());
		this.joinType = type;
	}

	public JoinType getJoinType() {
		return joinType;
	}

	@Override
	protected Set<JoinInfo> getJoinInfoSet() {
		return infos;
	}

	private void addInfos(Set<JoinInfo> set) {
		infos.addAll(set);
		
		for(JoinInfo info : set) {
			views.addAll(info.getViews());
		}
	}

	@Override
	protected Set<ViewInfo> getViews() {
		return views;
	}

}
