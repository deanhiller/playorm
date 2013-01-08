package com.alvazan.orm.parser.antlr;

import java.util.List;





public interface ParsedNode {

	int getType();

	ParsedNode getParent();

	ParsedNode getChild(ChildSide side);
	
	void setChild(ChildSide left, ParsedNode nodeToMove);

	String getAliasAndColumn();

	//For join optimization...
	ViewInfoImpl getViewInfo();
	boolean isAndOrType();
	void setJoinMeta(JoinMeta info);
	JoinMeta getJoinMeta();

	boolean isConstant();
	boolean isParameter();

	void replace(ParsedNode oldChild, ParsedNode newChild);
	ParsedNode getOppositeChild(ParsedNode first);

	boolean isBetweenExpression();

	List<ParsedNode> getChildrenForIn();
	void setChildrenForIn(List<ParsedNode> listOfNode);
	boolean isInExpression();
	
}
