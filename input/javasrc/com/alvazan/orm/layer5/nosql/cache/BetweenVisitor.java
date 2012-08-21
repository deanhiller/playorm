package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.layer5.indexing.ChildSide;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.indexing.StateAttribute;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class BetweenVisitor {

	private String tableAndColumnName;
	private ExpressionNode firstMatch;
	private ExpressionNode secondMatch;
	private ExpressionNode rootNode;
	
	public BetweenVisitor(String key) {
		this.tableAndColumnName = key;
	}

	public void setFirstMatch(ExpressionNode node) {
		firstMatch = node;
	}

	public ExpressionNode getFirstMatch() {
		return firstMatch;
	}

	public void setSecondMatch(ExpressionNode node) {
		this.secondMatch = node;
	}

	public ExpressionNode getSecondMatch() {
		return secondMatch;
	}

	public ExpressionNode walkAndFixTree(ExpressionNode node, String query) {
		this.rootNode = node;
		walkTree(rootNode, query);
		return rootNode;
	}
	
	public void walkTree(ExpressionNode node, String query) {
		if(node.getType() != NoSqlLexer.AND && node.getType() != NoSqlLexer.OR)
			return;  //We are not interested in other nodes, only AND and OR nodes so we can look at their children ourselves

		findProcessMatch(node, ChildSide.RIGHT);
		findProcessMatch(node, ChildSide.LEFT);
		
		ExpressionNode right = node.getRightChild();
		ExpressionNode left = node.getLeftChild();
		walkTree(right, query);
		walkTree(left, query);
	}

	private void findProcessMatch(ExpressionNode node, ChildSide side) {
		if(node.getType() != NoSqlLexer.AND)
			return; // nothing to do
		
		ExpressionNode match = findSidesMatch(node, side);
		if(match == null)
			return; //nothing to do
		
		if(getFirstMatch() == null)
			processFirstMatch(node, match, side);
		else if(getSecondMatch() == null)
			processSecondMatch(node, match);
		else
			throw new IllegalArgumentException("Your query uses the column="+this.tableAndColumnName
					+" 3 times in the query with AND every time.  This is not allowed as it only needs to be used twice in and clauses");
	}

	private ExpressionNode findSidesMatch(ExpressionNode node, ChildSide side) {
		ExpressionNode childNode = node.getChild(side);
		//attribute is always on the left side since we rewrite the tree
		ExpressionNode attributeNode = childNode.getLeftChild();
		//The parent must be an AND so we can try to find another reference to same variable that is ANDED with this guy
		if(attributeNode.getType() == NoSqlLexer.ATTR_NAME) {
			StateAttribute state = (StateAttribute) attributeNode.getState();
			String key = state.getTableName()+"-"+state.getColumnInfo().getColumnName();
			if(key.equals(tableAndColumnName))
				return childNode;
		}
		return null;
	}
	
	private void processSecondMatch(ExpressionNode node, ExpressionNode match) {
		setSecondMatch(match);
		ExpressionNode firstMatch = getFirstMatch();
		match.addExpression(firstMatch);
	}

	private void processFirstMatch(ExpressionNode node, ExpressionNode match, ChildSide side) {
		//let's play re-organize the tree now
		setFirstMatch(match);
		ExpressionNode nodeToMove = node.getRightChild();
		if(side == ChildSide.RIGHT) {
			nodeToMove = node.getLeftChild();
		}

		if(node == rootNode) {
			rootNode = nodeToMove;
		} else if(node.isLeftChildNode()) {
			node.getParent().setLeftChild(nodeToMove);
		} else if(node.isRightChildNode()) {
			node.getParent().setRightChild(nodeToMove);
		} else {
			throw new RuntimeException("bug, should not get here");
		}
	}
	
}
