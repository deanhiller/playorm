package com.alvazan.orm.layer5.indexing;

public class NodeTuple {

	private ExpressionNode node;
	private ChildSide side;

	public NodeTuple(ExpressionNode n, ChildSide side) {
		this.node = n;
		this.side = side;
	}

	public ExpressionNode getNode() {
		return node;
	}

	public void setNode(ExpressionNode node) {
		this.node = node;
	}

	public ChildSide getSide() {
		return side;
	}

	public void setSide(ChildSide side) {
		this.side = side;
	}
}
