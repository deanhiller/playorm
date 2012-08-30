package com.alvazan.orm.parser.antlr;

import com.alvazan.orm.layer5.indexing.JoinType;
import com.alvazan.orm.layer5.indexing.ViewInfo;
import com.alvazan.orm.layer5.nosql.cache.InfoForWiring;

public class AddJoinInfo {

	private ParsedNode root;

	public ParsedNode walkTree(ParsedNode originalRoot, InfoForWiring wiring) {
		this.root = originalRoot;
		walkTheTree(originalRoot, wiring);
		return this.root;
	}

	private void walkTheTree(ParsedNode node, InfoForWiring wiring) {
		ParsedNode left = node.getChild(ChildSide.LEFT);
		ParsedNode right = node.getChild(ChildSide.RIGHT);
		if(node.isAndOrType()) {
			walkTheTree(left, wiring);
			walkTheTree(right, wiring);
		} else {
			//let's find out the join type
			JoinMeta meta = findDirectJoin(wiring, left, right);
			node.setJoinMeta(meta);
			return;
		}
		
		JoinMeta rightType = right.getJoinMeta();
		JoinMeta leftType = left.getJoinMeta();
		JoinMeta info = rightType.fetchJoinMeta(leftType);
		node.setJoinMeta(info);
	}

	private JoinMeta findDirectJoin(InfoForWiring wiring, ParsedNode left, ParsedNode right) {
		//At this point, we know the left will be a table, but the rightside could be a constant or another table or same table
		ViewInfo view1 = left.getViewInfo();
		if(right.isConstant() || right.isParameter()) {
			JoinInfo info = new JoinInfo(view1, null, null, null, JoinType.NONE);
			return new JoinMetaComposite(info, info.getJoinType());
		}
		
		//okay, table vs. table then
		ViewInfo view2 = right.getViewInfo();
		if(view1.equals(view2)) {
			JoinInfo info = new JoinInfo(view1, null, null, null, JoinType.NONE);
			return new JoinMetaComposite(info, info.getJoinType());
		}
		
		JoinInfo info = view1.getJoinInfo(view2);
		if(info == null)
			throw new IllegalArgumentException("Sorry, but you have a and/or clause on alias="+view1.getAlias()+" and alias="+view2.getAlias()+
					" where the two tables have another join between them that needs " +
					"to happen first.  Rewrite your query. (ie. something like b&(c or a)" +
					" needs to be rewritten to b&c or b&a as b is in the middle");
		
		JoinMetaComposite comp = new JoinMetaComposite(info, info.getJoinType());
		return comp;
	}

}
