package com.alvazan.orm.parser.antlr;

import com.alvazan.orm.layer5.indexing.ViewInfo;



public interface ParsedNode {

	int getType();

	ParsedNode getParent();

	ParsedNode getChild(ChildSide side);
	
	void setChild(ChildSide left, ParsedNode nodeToMove);

	boolean isChildOnSide(ChildSide side);

	void addExpression(ParsedNode firstMatch);

	String getAliasAndColumn();

	//For join optimization...
	ViewInfo getViewInfo();
	boolean isAndOrType();
	void setJoinMeta(JoinMeta info);
	JoinMeta getJoinMeta();
	
}
