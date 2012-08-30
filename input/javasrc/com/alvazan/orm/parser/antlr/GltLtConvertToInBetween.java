package com.alvazan.orm.parser.antlr;


public class GltLtConvertToInBetween {

	private String aliasAndColumnName;
	private ParsedNode firstMatch;
	private ParsedNode secondMatch;
	private ParsedNode rootNode;
	
	public GltLtConvertToInBetween(String aliasAndColumnName) {
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

	public ParsedNode walkAndFixTree(ParsedNode node, String query) {
		this.rootNode = node;
		walkTree(rootNode, query);
		return rootNode;
	}
	
	public void walkTree(ParsedNode node, String query) {
		if(node.getType() != NoSqlLexer.AND && node.getType() != NoSqlLexer.OR)
			return;  //We are not interested in other nodes, only AND and OR nodes so we can look at their children ourselves

		findProcessMatch(node, ChildSide.RIGHT);
		findProcessMatch(node, ChildSide.LEFT);
		
		ParsedNode right = node.getChild(ChildSide.RIGHT);
		ParsedNode left = node.getChild(ChildSide.LEFT);
		walkTree(right, query);
		walkTree(left, query);
	}

	private void findProcessMatch(ParsedNode node, ChildSide side) {
		if(node.getType() != NoSqlLexer.AND)
			return; // nothing to do
		
		ParsedNode match = findSidesMatch(node, side);
		if(match == null)
			return; //nothing to do
		
		if(getFirstMatch() == null)
			processFirstMatch(node, match, side);
		else if(getSecondMatch() == null)
			processSecondMatch(node, match);
		else
			throw new IllegalArgumentException("Your query uses the column="+aliasAndColumnName
					+" 3 times in the query with AND every time.  This is not allowed as it only needs to be used twice in and clauses");
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
	
	private void processSecondMatch(ParsedNode node, ParsedNode match) {
		setSecondMatch(match);
		ParsedNode firstMatch = getFirstMatch();
		match.addExpression(firstMatch);
	}

	private void processFirstMatch(ParsedNode node, ParsedNode match, ChildSide side) {
		//let's play re-organize the tree now
		setFirstMatch(match);
		ParsedNode nodeToMove = node.getChild(ChildSide.RIGHT);
		if(side == ChildSide.RIGHT) {
			nodeToMove = node.getChild(ChildSide.LEFT);
		}

		if(node == rootNode) {
			rootNode = nodeToMove;
		} else if(node.isChildOnSide(ChildSide.LEFT)) {
			node.getParent().setChild(ChildSide.LEFT, nodeToMove);
		} else if(node.isChildOnSide(ChildSide.RIGHT)) {
			node.getParent().setChild(ChildSide.RIGHT, nodeToMove);
		} else {
			throw new RuntimeException("bug, should not get here");
		}
	}
	
}
