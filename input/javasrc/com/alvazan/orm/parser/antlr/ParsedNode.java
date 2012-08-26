package com.alvazan.orm.parser.antlr;


public interface ParsedNode {

	int getType();

	ParsedNode getParent();

	ParsedNode getChild(ChildSide side);
	
	void setChild(ChildSide left, ParsedNode nodeToMove);

	boolean isChildOnSide(ChildSide side);

	void addExpression(ParsedNode firstMatch);

	String getAliasAndColumn();

}
