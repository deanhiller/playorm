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
			
			JoinMeta rightType = right.getJoinMeta();
			JoinMeta leftType = left.getJoinMeta();
			JoinMeta info = rightType.fetchJoinMeta(leftType);
			node.setJoinMeta(info);
			return;
			
		} else if(node.isInBetweenExpression()) {
			ParsedNode leftGrand = left.getChild(ChildSide.LEFT);
			ViewInfo viewInfo = leftGrand.getViewInfo();
			JoinInfo info = new JoinInfo(viewInfo, null, null, null, JoinType.NONE);
			JoinMeta meta1 = new JoinMeta(info, info.getJoinType());
			node.setJoinMeta(meta1);
			return;
		}
		
		//let's find out the join type
		JoinMeta meta = findDirectJoin(left, right);
		node.setJoinMeta(meta);
	}

	private JoinMeta findDirectJoin(ParsedNode left, ParsedNode right) {
		//At this point, we know the left will be a table, but the rightside could be a constant or another table or same table
		ViewInfo view1 = left.getViewInfo();
		if(right.isConstant() || right.isParameter()) {
			JoinInfo info = new JoinInfo(view1, null, null, null, JoinType.NONE);
			return new JoinMeta(info, info.getJoinType());
		}
		
		//okay, table vs. table then
		ViewInfo view2 = right.getViewInfo();
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
