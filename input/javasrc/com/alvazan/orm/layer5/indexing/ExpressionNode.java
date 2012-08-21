package com.alvazan.orm.layer5.indexing;

import org.antlr.runtime.tree.CommonTree;

import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class ExpressionNode {

	private ExpressionNode leftChild;
	private ExpressionNode rightChild;
	private CommonTree commonNode;
	
	private Object state;
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

	public int getType() {
		return commonNode.getType();		
	}
	
	public CommonTree getASTNode() {
		return commonNode;
	}

	private void setParent(ExpressionNode p) {
		this.parent = p;
	}
	
	public void setLeftChild(ExpressionNode childNode) {
		leftChild = childNode;
		leftChild.setParent(this);
	}
	
	public void setRightChild(ExpressionNode child) {
		rightChild = child;
		rightChild.setParent(this);
		this.commonNode.setChild(1, child.commonNode);
	}

	public void setState(Object state) {
		this.state = state;
	}

	public ExpressionNode getLeftChild() {
		return leftChild;
	}

	public ExpressionNode getRightChild() {
		return rightChild;
	}

	public Object getState() {
		return state;
	}


	public String getExpressionAsString(boolean isCalledFromFirstIfBlock) {
		if(greaterThanExpression != null && !isCalledFromFirstIfBlock) {
			//This one is a bit tough, as we can't toString on ONE of the two greaterThanExpression or lessThanExpression as that would be OURSELF and
			//we woudl infinitely recurse into the getExpressionAsString function :(
			String newSign = "<";
			if(greaterThanExpression.getType() == NoSqlLexer.GE)
				newSign = "<=";
			
			String lessThanExpr = "";
			if(lessThanExpression == this) {
				//avoid recursion because of the if(greaterThanExpression above which would always not be null in this case since we reference ourselves
				lessThanExpr = getExpressionAsString(true);
			} else {
				lessThanExpr = lessThanExpression+"";
			}
			
			ExpressionNode rightChild = greaterThanExpression.getRightChild();
			String line = rightChild+newSign+lessThanExpr;
			return line;
		} else if(leftChild != null && rightChild != null) {
			String msg = leftChild.getExpressionAsString(false)+" "+this.commonNode+" "+rightChild.getExpressionAsString(false);
			if(getType() == NoSqlLexer.AND || getType() == NoSqlLexer.OR)
				return "("+msg+")";
			return msg;
		}
		return this.commonNode+"["+commonNode.getType()+"]";
	}

	@Override
	public String toString() {
		return getExpressionAsString(false);
	}

	public boolean isRightChildNode() {
		if(getParent() == null)
			return false;
		else if(getParent().getRightChild() == this)
			return true;
		return false;
	}

	public boolean isLeftChildNode() {
		if(getParent() == null)
			return false;
		else if(getParent().getLeftChild() == this)
			return true;
		return false;
	}

	public ExpressionNode getParent() {
		return parent;
	}

	public void addExpression(ExpressionNode attrExpNode) {
		String msg = "this type="+this.getType()+" node type="+attrExpNode.getType();
		ExpressionNode attributeSideNode = attrExpNode.getLeftChild();
		StateAttribute state2 = (StateAttribute) attributeSideNode.getState();
		if(attrExpNode.getType() == NoSqlLexer.EQ || this.getType() == NoSqlLexer.EQ) {
			throw new IllegalArgumentException("uhhhmmmm, you are using column="+state2.getColumnInfo().getColumnName()
					+" twice with 'AND' statement yet one has an = so change to 'OR' or get rid of one. "+ msg);
		} else if(attrExpNode.getType() == NoSqlLexer.GE || attrExpNode.getType() == NoSqlLexer.GT) {
			greaterThanExpression = attrExpNode;
			lessThanExpression = this;
		} else if(this.getType() == NoSqlLexer.GE || this.getType() == NoSqlLexer.GT) {
			greaterThanExpression = this;
			lessThanExpression = attrExpNode;
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
