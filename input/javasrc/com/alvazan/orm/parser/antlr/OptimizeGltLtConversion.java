package com.alvazan.orm.parser.antlr;



public class OptimizeGltLtConversion {

	private String aliasAndColumnName;
	private ParsedNode firstMatch;
	private ParsedNode secondMatch;
	private ParsedNode rootNode;
	
	public OptimizeGltLtConversion(String aliasAndColumnName) {
		this.aliasAndColumnName = aliasAndColumnName;
	}

	public void setFirstMatch(ParsedNode node) {
		firstMatch = node;
	}

	public ParsedNode getFirstMatch() {
		return firstMatch;
	}

	public void setSecondMatch(ParsedNode node) {
		this.secondMatch = node;
	}

	public ParsedNode getSecondMatch() {
		return secondMatch;
	}

	public ParsedNode walkAndFixTree(ParsedNode node, String query, MetaFacade facade) {
		this.rootNode = node;
		walkTree(rootNode, query, facade);
		return rootNode;
	}
	
	public void walkTree(ParsedNode node, String query, MetaFacade facade) {
		if(node.getType() != NoSqlLexer.AND && node.getType() != NoSqlLexer.OR)
			return;  //We are not interested in other nodes, only AND and OR nodes so we can look at their children ourselves

		findProcessMatch(node, ChildSide.RIGHT, facade);
		findProcessMatch(node, ChildSide.LEFT, facade);
		
		ParsedNode right = node.getChild(ChildSide.RIGHT);
		ParsedNode left = node.getChild(ChildSide.LEFT);
		walkTree(right, query, facade);
		walkTree(left, query, facade);
	}

	private void findProcessMatch(ParsedNode node, ChildSide side, MetaFacade facade) {
		if(node.getType() != NoSqlLexer.AND)
			return; // nothing to do
		
		ParsedNode match = findSidesMatch(node, side);
		if(match == null)
			return; //nothing to do
		
		if(getFirstMatch() == null)
			processFirstMatch(node, match, side);
		else if(getSecondMatch() == null)
			processSecondMatch(node, match, facade);
		else
			throw new IllegalArgumentException("Your query uses the column="+aliasAndColumnName
					+" 3 times in the query with AND every time.  This is not allowed as it only needs to be used twice in the 'and' clauses");
	}

	private ParsedNode findSidesMatch(ParsedNode node, ChildSide side) {
		ParsedNode childNode = node.getChild(side);
		//attribute is always on the left side since we rewrite the tree
		ParsedNode attributeNode = childNode.getChild(ChildSide.LEFT);
		//The parent must be an AND so we can try to find another reference to same
		//variable that is ANDED with this guy
		if(attributeNode.getType() == NoSqlLexer.ATTR_NAME) {
			String aliasAndCol = attributeNode.getAliasAndColumn();
			if(aliasAndColumnName.equals(aliasAndCol))
				return childNode;
		}
		return null;
	}
	
	private void processSecondMatch(ParsedNode node, ParsedNode match, MetaFacade facade) {
		setSecondMatch(match);
		
		ParsedNode betweenExpr = facade.createExpression(NoSqlLexer.BETWEEN);
		ParsedNode first = getFirstMatch();
		match.getParent().replace(match, betweenExpr);
		delete(first);
		
		addExpression(betweenExpr, first, match);
	}

	private void delete(ParsedNode first) {
		ParsedNode parent = first.getParent();
		ParsedNode nodeToMove = parent.getOppositeChild(first);
		if(parent == rootNode) {
			//If we are the root node, the tree is now collapsing and removing root node
			rootNode = nodeToMove;
		} else {
			parent.replace(first, nodeToMove);
		}
	}

	private void processFirstMatch(ParsedNode node, ParsedNode match, ChildSide side) {
		//let's cache for tree organization later
		setFirstMatch(match);

	}
	public void addExpression(ParsedNode betweenExpr, ParsedNode firstMatch, ParsedNode secondMatch) {
		ParsedNode leftAttributeSide = firstMatch.getChild(ChildSide.LEFT);
		String aliasAndColumn = leftAttributeSide.getAliasAndColumn();

		if(firstMatch.getType() == NoSqlLexer.EQ || secondMatch.getType() == NoSqlLexer.EQ) {
			throw new IllegalArgumentException("uhhhmmmm, you are using column="+aliasAndColumn
					+" twice with 'AND' statement yet one has an = so change to 'OR' or get rid of one. ");
		} else if(firstMatch.getType() == NoSqlLexer.GE || firstMatch.getType() == NoSqlLexer.GT) {
			betweenExpr.setChild(ChildSide.LEFT, firstMatch);
			betweenExpr.setChild(ChildSide.RIGHT, secondMatch);
			if(secondMatch.getType() == NoSqlLexer.GE || secondMatch.getType() == NoSqlLexer.GT)
				throw new IllegalArgumentException("You are using > or >= twice on the same column so delete one of those in your query so we don't have to");
		} else if(firstMatch.getType() == NoSqlLexer.LE || firstMatch.getType() == NoSqlLexer.LT) {
			betweenExpr.setChild(ChildSide.LEFT, secondMatch);
			betweenExpr.setChild(ChildSide.RIGHT, firstMatch);
			if(secondMatch.getType() == NoSqlLexer.LE || secondMatch.getType() == NoSqlLexer.LT)
				throw new IllegalArgumentException("uhmmmm, you are using column="+aliasAndColumn+" twice(which is fine) but both of them are using less than, delete one of them. ");
		} else
			throw new RuntimeException("bug, we should never get here but this should be easy to fix.  ");
	}
}
