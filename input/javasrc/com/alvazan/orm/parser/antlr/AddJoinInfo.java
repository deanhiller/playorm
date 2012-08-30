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
		if(left.isAndOrType()) {
			walkTheTree(left, wiring);
		}
		if(right.isAndOrType()) {
			walkTheTree(right, wiring);
		}
		
		if(!left.isAndOrType() && !right.isAndOrType()) {
			//let's find out the join type
			JoinMeta meta = findSimpleJoin(wiring, left, right);
			node.setJoinMeta(meta);
		} else {
			JoinMeta rightType = right.getJoinMeta();
			JoinMeta leftType = left.getJoinMeta();
			JoinMeta info = rightType.fetchJoinMeta(leftType);
			node.setJoinMeta(info);
		}
	}

	private JoinMeta findSimpleJoin(InfoForWiring wiring, ParsedNode left,
			ParsedNode right) {
		ViewInfo table1 = left.getViewInfo();
		ViewInfo table2 = right.getViewInfo();
		if(table1.equals(table2)) {
			JoinInfo info = new JoinInfo(table1, null, null, null, JoinType.NONE);
			return new JoinMetaComposite(info, info.getJoinType());
		}
		
		JoinInfo info = table1.getJoinInfo(table2);
		if(info == null)
			throw new IllegalArgumentException("Sorry, but you have a and/or clause on alias="+table1.getAlias()+" and alias="+table2.getAlias()+
					" where the two tables have another join between them that needs " +
					"to happen first.  Rewrite your query. (ie. something like b&(c or a)" +
					" needs to be rewritten to b&c or b&a as b is in the middle");
		
		JoinMetaComposite comp = new JoinMetaComposite(info, info.getJoinType());
		return comp;
	}

}
