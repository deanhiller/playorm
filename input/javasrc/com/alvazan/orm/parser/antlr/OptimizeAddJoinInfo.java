package com.alvazan.orm.parser.antlr;



public class OptimizeAddJoinInfo {

	private ParsedNode root;

	public ParsedNode walkTree(ParsedNode originalRoot) {
		this.root = originalRoot;
		walkTheTree(originalRoot);
		return this.root;
	}

	private void walkTheTree(ParsedNode node) {
		ParsedNode left = node.getChild(ChildSide.LEFT);
		ParsedNode right = node.getChild(ChildSide.RIGHT);
		if(node.isAndOrType()) {
			walkTheTree(left);
			walkTheTree(right);
			
			JoinMeta info = switchChildrenIfNeeded(node, left, right);
			
			node.setJoinMeta(info);
			return;
			
		} else if(node.isInBetweenExpression()) {
			ParsedNode leftGrand = left.getChild(ChildSide.LEFT);
			ViewInfoImpl viewInfo = leftGrand.getViewInfo();
			JoinInfo info = new JoinInfo(viewInfo, null, null, null, JoinType.NONE);
			JoinMeta meta1 = new JoinMeta(info, info.getJoinType());
			node.setJoinMeta(meta1);
			return;
		}
		
		//let's find out the join type
		JoinMeta meta = findDirectJoin(left, right);
		node.setJoinMeta(meta);
	}

	private JoinMeta switchChildrenIfNeeded(ParsedNode node, ParsedNode left,
			ParsedNode right) {
		JoinMeta leftType = left.getJoinMeta();
		JoinMeta rightType = right.getJoinMeta();
		JoinMeta info = leftType.fetchJoinMeta(rightType);
		JoinInfo primary = info.getPrimaryJoinInfo();
		if(primary.getJoinType() != JoinType.NONE) {
			ViewInfoImpl primaryTable = primary.getPrimaryTable();
			if(!rightType.contains(primaryTable)) {
				//we need to rewrite the tree so the joining table is on the left(right now, joining is the right side)
				checkNoSidesHavePrimaryView(leftType, primaryTable);
				node.setChild(ChildSide.LEFT, right);
				node.setChild(ChildSide.RIGHT, left);
			} else if(leftType.contains(primaryTable))
				throw new RuntimeException("bug, should never get here.  Both sides should not both contain the primary table unless jointype=NONE");
		}
		return info;
	}

	private void checkNoSidesHavePrimaryView(JoinMeta leftType,
			ViewInfoImpl primaryTable) {
		if(!leftType.contains(primaryTable))
			throw new RuntimeException("bug, should never get here, one side should have primarytable or we can't join");
	}

	private JoinMeta findDirectJoin(ParsedNode left, ParsedNode right) {
		//At this point, we know the left will be a table, but the rightside could be a constant or another table or same table
		ViewInfoImpl view1 = left.getViewInfo();
		if(right.isConstant() || right.isParameter()) {
			JoinInfo info = new JoinInfo(view1, null, null, null, JoinType.NONE);
			return new JoinMeta(info, info.getJoinType());
		}
		
		//okay, table vs. table then
		ViewInfoImpl view2 = right.getViewInfo();
		if(view1.equals(view2)) {
			JoinInfo info = new JoinInfo(view1, null, null, null, JoinType.NONE);
			return new JoinMeta(info, info.getJoinType());
		}
		
		JoinInfo info = view1.getJoinInfo(view2);
		if(info == null)
			throw new IllegalArgumentException("Sorry, but you have a and/or clause on alias="+view1.getAlias()+" and alias="+view2.getAlias()+
					" where the two tables have another join between them that needs " +
					"to happen first.  Rewrite your query. (ie. something like b&(c or a)" +
					" needs to be rewritten to b&c or b&a as b is in the middle");
		
		JoinMeta comp = new JoinMeta(info, info.getJoinType());
		return comp;
	}

}
