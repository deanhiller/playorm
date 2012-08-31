package com.alvazan.orm.parser.antlr;

import org.antlr.runtime.tree.CommonTree;

public class ExpressionNode implements ParsedNode {

	/**
	 * In the case where one has a query of where x.size > 5 and x.size < 10, and if this is the x.size > 5 node, we add the x.size < 10 node
	 * to this node so we can do range queries for an optimization to narrow the search down.
	 */
	private ExpressionNode leftChild;
	private ExpressionNode rightChild;
	
	private CommonTree commonNode;
	
	private Object state;
	private String textInSql;
	private ExpressionNode parent;
	private JoinMeta joinMeta;
	private int type;
	
	public ExpressionNode(CommonTree expression) {
		this.type = expression.getType();
		this.commonNode = expression;
	}

	public ExpressionNode(int nodeType) {
		this.type = nodeType;
	}

	public boolean isInBetweenExpression() {
		return type == NoSqlLexer.BETWEEN;
	}

	public int getType() {
		return type;	
	}
	
	public CommonTree getASTNode() {
		return commonNode;
	}

	private void setParent(ExpressionNode p) {
		this.parent = p;
	}
	
	public void setChild(ChildSide side, ParsedNode child2) {
		ExpressionNode child = (ExpressionNode) child2;
		if(side == ChildSide.LEFT) {
			leftChild = child;
		} else {
			rightChild = child;
		}
		child.setParent(this);
	}

	public void setState(Object state, String textInSql) {
		this.state = state;
		this.textInSql = textInSql;
	}

	@Override
	public String getAliasAndColumn() {
		if(!(state instanceof StateAttribute))
			return null;
		StateAttribute attr = (StateAttribute) state;
		return attr.getTextInSql();
	}
	
	public Object getState() {
		return state;
	}


	public String getExpressionAsString(boolean isCalledFromFirstIfBlock) {
		if(isInBetweenExpression()) {
			ExpressionNode greaterThan = getGreaterThan();
			ExpressionNode lessThan = getLessThan();
			if(greaterThan == null || lessThan == null)
				return "(between not filled in yet)";
			
			//This one is a bit tough, as we can't toString on ONE of the two greaterThanExpression or lessThanExpression as that would be OURSELF and
			//we woudl infinitely recurse into the getExpressionAsString function :(
			String greaterThanSign = " < ";
			if(greaterThan.getType() == NoSqlLexer.GE)
				greaterThanSign = " <= ";
			
			String lessThanSign = " < ";
			if(lessThan.getType() == NoSqlLexer.LE)
				lessThanSign = " <= ";

			ExpressionNode greaterThanLeftCol = greaterThan.getChild(ChildSide.LEFT);
			ExpressionNode greaterThanRightVar = greaterThan.getChild(ChildSide.RIGHT);
			ExpressionNode lessThanRightVar = lessThan.getChild(ChildSide.RIGHT);
			
			String line = greaterThanRightVar+greaterThanSign+greaterThanLeftCol+lessThanSign+lessThanRightVar;
			return line;
		} else if(leftChild != null && rightChild != null) {
			String joinType = "";
			if(joinMeta != null) {
				if(joinMeta.getJoinType() == JoinType.INNER)
					joinType = "(innerjoin)";
			}
			
			String msg = leftChild.getExpressionAsString(false)+" "+this.commonNode+joinType+" "+rightChild.getExpressionAsString(false);
			if(getType() == NoSqlLexer.AND || getType() == NoSqlLexer.OR)
				return "("+msg+")";
			return msg;
		}
		
		return textInSql;
	}

	@Override
	public String toString() {
		return getExpressionAsString(false);
	}
	
	public ExpressionNode getParent() {
		return parent;
	}

	public ExpressionNode getChild(ChildSide side) {
		if(side == ChildSide.RIGHT)
			return rightChild;
			
		return leftChild;
	}

	public ExpressionNode getGreaterThan() {
		return leftChild;
	}

	public ExpressionNode getLessThan() {
		return rightChild;
	}

	@Override
	public ViewInfo getViewInfo() {
		if(!(state instanceof StateAttribute))
			throw new IllegalStateException("This node is of the wrong type="+this.commonNode.getType());
		StateAttribute attr = (StateAttribute) state;
		return attr.getViewInfo();
	}

	@Override
	public boolean isAndOrType() {
		if(commonNode == null)
			return false;
		if(commonNode.getType() == NoSqlLexer.AND || commonNode.getType() == NoSqlLexer.OR)
			return true;
		return false;
	}

	@Override
	public void setJoinMeta(JoinMeta meta) {
		this.joinMeta = meta;
	}

	@Override
	public JoinMeta getJoinMeta() {
		return joinMeta;
	}

	@Override
	public boolean isConstant() {
		if(getType() == NoSqlLexer.DEC_VAL || getType() == NoSqlLexer.INT_VAL
				|| getType() == NoSqlLexer.STR_VAL)
			return true;
		return false;
	}

	@Override
	public boolean isParameter() {
		if(getType() == NoSqlLexer.PARAMETER_NAME)
			return true;
		return false;
	}

	@Override
	public void replace(ParsedNode oldChild, ParsedNode newChild) {
		if(oldChild == leftChild) {
			setChild(ChildSide.LEFT, newChild);
		} else if(oldChild == rightChild) {
			setChild(ChildSide.RIGHT, newChild);
		}
		else
			throw new IllegalArgumentException("bug, hmmm, oldChild was not a child of this node, what the?");
	}

	@Override
	public ParsedNode getOppositeChild(ParsedNode child) {
		if(child == leftChild)
			return rightChild;
		else if(child == rightChild)
			return leftChild;
		throw new IllegalStateException("bug, should never end up here");
	}

}
