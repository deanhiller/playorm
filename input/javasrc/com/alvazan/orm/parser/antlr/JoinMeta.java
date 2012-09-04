package com.alvazan.orm.parser.antlr;

import java.util.HashSet;
import java.util.Set;



public class JoinMeta {

	private JoinInfo primaryJoinInfo;
	private Set<JoinInfo> infos = new HashSet<JoinInfo>();
	private Set<ViewInfoImpl> views = new HashSet<ViewInfoImpl>();
	private JoinType joinType;

	public JoinMeta(JoinInfo info, JoinType type, Set<JoinInfo> set1, Set<JoinInfo> set2) {
		this(info, type);
		addInfos(set1);
		addInfos(set2);
	}
	
	public JoinMeta(JoinInfo info, JoinType type) {
		this.primaryJoinInfo = info;
		this.infos.add(info);
		views.addAll(info.getViews());
		this.joinType = type;
	}
	
	@Override
	public String toString() {
		return "["+joinType+",views="+views+", primary="+primaryJoinInfo+"]";
	}

	
	public JoinInfo getPrimaryJoinInfo() {
		return primaryJoinInfo;
	}

	public JoinMeta fetchJoinMeta(JoinMeta rightSide) {
		Set<ViewInfoImpl> views1 = this.getViews();
		Set<ViewInfoImpl> views2 = rightSide.getViews();
		//View matches are MUCH less costly than join matches so find view match first!!
		for(ViewInfoImpl infoL : views1) {
			for(ViewInfoImpl infoR : views2) {
				if(infoL == infoR) {
					JoinInfo joinInfo = new JoinInfo(infoL, null, null, null, JoinType.NONE);
					Set<JoinInfo> set1 = this.getJoinInfoSet();
					Set<JoinInfo> set2 = rightSide.getJoinInfoSet();
					JoinMeta meta = new JoinMeta(joinInfo, JoinType.NONE, set1, set2);
					return meta;
				}
			}
		}
		
		for(ViewInfoImpl infoL : views1) {
			for(ViewInfoImpl infoR : views2) {
				JoinInfo joinInfo = infoL.getJoinInfo(infoR);
				if(joinInfo != null) {
					Set<JoinInfo> set1 = this.getJoinInfoSet();
					Set<JoinInfo> set2 = rightSide.getJoinInfoSet();
					JoinMeta meta = new JoinMeta(joinInfo, joinInfo.getJoinType(), set1, set2);
					return meta;
				}
			}
		}
		
		throw new IllegalArgumentException("Sorry, if you got here, you have a complex query that is not optimized.  " +
				"We were trying to find a direct join between infos="+views1+" and infos="+views2+" but could not find a direct" +
						" join so either 1. optimize the query, 2. send us the query so we can send you the optimized version" +
						" or 3. wait for us to implement the optimizer");
	}

	public JoinType getJoinType() {
		return joinType;
	}

	protected Set<JoinInfo> getJoinInfoSet() {
		return infos;
	}

	private void addInfos(Set<JoinInfo> set) {
		infos.addAll(set);
		
		for(JoinInfo info : set) {
			views.addAll(info.getViews());
		}
	}

	protected Set<ViewInfoImpl> getViews() {
		return views;
	}

	public boolean contains(ViewInfoImpl primaryTable) {
		return views.contains(primaryTable);
	}
}
