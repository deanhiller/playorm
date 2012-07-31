package com.alvazan.orm.api.spi3.index;

import org.antlr.runtime.tree.CommonTree;

public class ExpressionNode {

	private ExpressionNode leftChild;
	private ExpressionNode rightChild;
	private CommonTree node;
	
	private Object state;
	
	public ExpressionNode(CommonTree expression) {
		this.node = expression;
	}

	public int getType() {
		return node.getType();
		
	}
	public CommonTree getASTNode() {
		return node;
	}

	public void setLeftChild(ExpressionNode childNode) {
		leftChild = childNode;
	}
	public void setRightChild(ExpressionNode child) {
		rightChild = child;
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


	public String getExpressionAsString() {
		if(leftChild != null && rightChild != null) {
			return "("+leftChild.getExpressionAsString()+" "+this.node+" "+rightChild.getExpressionAsString()+")";
		}
		return this.node+"("+node.getType()+")";
	}
	

}
