package com.alvazan.orm.layer5.indexing;

import org.antlr.runtime.tree.CommonTree;

import com.alvazan.orm.parser.antlr.ChildSide;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.ParsedNode;

public class ExpressionNode implements ParsedNode {

	private NodeType nodeType;
	private ExpressionNode leftChild;
	private ExpressionNode rightChild;
	private CommonTree commonNode;
	
	private Object state;
	private String textInSql;
	private ExpressionNode parent;
	/**
	 * In the case where one has a query of where x.size > 5 and x.size < 10, and if this is the x.size > 5 node, we add the x.size < 10 node
	 * to this node so we can do range queries for an optimization to narrow the search down.
	 */
	private ExpressionNode greaterThanExpression;
	private ExpressionNode lessThanExpression;
	
	public ExpressionNode(CommonTree expression) {
		this.commonNode = expression;
	}

	public boolean isInBetweenExpression() {
		return greaterThanExpression != null;
	}

	public NodeType getNodeType() {
		return nodeType;
	}

	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}

	public int getType() {
		return commonNode.getType();		
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
			this.commonNode.setChild(0, leftChild.commonNode);
		} else {
			rightChild = child;
			this.commonNode.setChild(1, rightChild.commonNode);
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
		if(greaterThanExpression != null && !isCalledFromFirstIfBlock) {
			//This one is a bit tough, as we can't toString on ONE of the two greaterThanExpression or lessThanExpression as that would be OURSELF and
			//we woudl infinitely recurse into the getExpressionAsString function :(
			String newSign = " < ";
			if(greaterThanExpression.getType() == NoSqlLexer.GE)
				newSign = " <= ";
			
			String lessThanExpr = "";
			if(lessThanExpression == this) {
				//avoid recursion because of the if(greaterThanExpression above which would always not be null in this case since we reference ourselves
				lessThanExpr = getExpressionAsString(true);
			} else {
				lessThanExpr = lessThanExpression+"";
			}
			
			ExpressionNode rightChild = greaterThanExpression.getChild(ChildSide.RIGHT);
			String line = rightChild+newSign+lessThanExpr;
			return line;
		} else if(leftChild != null && rightChild != null) {
			String msg = leftChild.getExpressionAsString(false)+" "+this.commonNode+" "+rightChild.getExpressionAsString(false);
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

	@Override
	public boolean isChildOnSide(ChildSide side) {
		if(getParent() == null)
			return false;
		else if(getParent().getChild(side) == this)
			return true;
		return false;
	}
	
	public ExpressionNode getParent() {
		return parent;
	}

	public void addExpression(ParsedNode attrExpNode) {
		String msg = "this type="+this.getType()+" node type="+attrExpNode.getType();
		ExpressionNode attributeSideNode = (ExpressionNode) attrExpNode.getChild(ChildSide.LEFT);
		StateAttribute state2 = (StateAttribute) attributeSideNode.getState();
		if(attrExpNode.getType() == NoSqlLexer.EQ || this.getType() == NoSqlLexer.EQ) {
			throw new IllegalArgumentException("uhhhmmmm, you are using column="+state2.getColumnInfo().getColumnName()
					+" twice with 'AND' statement yet one has an = so change to 'OR' or get rid of one. "+ msg);
		} else if(attrExpNode.getType() == NoSqlLexer.GE || attrExpNode.getType() == NoSqlLexer.GT) {
			greaterThanExpression = (ExpressionNode) attrExpNode;
			lessThanExpression = this;
		} else if(this.getType() == NoSqlLexer.GE || this.getType() == NoSqlLexer.GT) {
			greaterThanExpression = this;
			lessThanExpression = (ExpressionNode) attrExpNode;
		} else if(this.getType() == NoSqlLexer.LE || this.getType() == NoSqlLexer.LT) {
			throw new IllegalArgumentException("uhmmmm, you are using column="+state2.getColumnInfo()+" twice(which is fine) but both of them are using greater than, delete one of them. " +msg);
		} else
			throw new RuntimeException("bug, we should never get here but this should be easy to fix.  "+msg);
	}

	public ExpressionNode getChild(ChildSide side) {
		if(side == ChildSide.RIGHT)
			return rightChild;
			
		return leftChild;
	}

	public ExpressionNode getGreaterThan() {
		return greaterThanExpression;
	}

	public ExpressionNode getLessThan() {
		return lessThanExpression;
	}



}
