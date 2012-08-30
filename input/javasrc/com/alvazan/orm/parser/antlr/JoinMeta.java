package com.alvazan.orm.parser.antlr;

import java.util.Set;

import com.alvazan.orm.layer5.indexing.JoinType;
import com.alvazan.orm.layer5.indexing.ViewInfo;

public abstract class JoinMeta {

	public JoinMeta fetchJoinMeta(JoinMeta rightSide) {
		Set<ViewInfo> views1 = this.getViews();
		Set<ViewInfo> views2 = rightSide.getViews();
		//View matches are MUCH less costly than join matches so find view match first!!
		for(ViewInfo infoL : views1) {
			for(ViewInfo infoR : views2) {
				JoinInfo joinInfo = infoL.findViewMatch(infoR);
				if(joinInfo != null) {
					Set<JoinInfo> set1 = this.getJoinInfoSet();
					Set<JoinInfo> set2 = rightSide.getJoinInfoSet();
					JoinMetaComposite meta = new JoinMetaComposite(joinInfo, JoinType.NONE, set1, set2);
					return meta;
				}
			}
		}
		
		for(ViewInfo infoL : views1) {
			for(ViewInfo infoR : views2) {
				JoinInfo joinInfo = infoL.getJoinInfo(infoR);
				if(joinInfo != null) {
					Set<JoinInfo> set1 = this.getJoinInfoSet();
					Set<JoinInfo> set2 = rightSide.getJoinInfoSet();
					JoinMetaComposite meta = new JoinMetaComposite(joinInfo, JoinType.NONE, set1, set2);
					return meta;
				}
			}
		}
		
		throw new IllegalArgumentException("Sorry, if you got here, you have a complex query that is not optimized.  " +
				"We were trying to find a direct join between infos="+views1+" and infos="+views2+" but could not find a direct" +
						" join so either 1. optimize the query, 2. send us the query so we can send you the optimized version" +
						" or 3. wait for us to implement the optimizer");
	}

	protected abstract Set<ViewInfo> getViews();

	protected abstract Set<JoinInfo> getJoinInfoSet();
}
