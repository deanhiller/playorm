package com.alvazan.orm.parser.tree;


/**
 * 
 * @author Huai Jiang
 *
 */
public class FilterExpression  implements Node{

    public enum Hyphen {
        OR,AND,GT,EQ,NE,LT,GE,LE
    }
    

	private Hyphen hyphen;


    private Node leftNode;
    
    
    private Node rightNode; 

    
    
    public Node getLeftNode() {
		return leftNode;
	}

	public void setLeftNode(Node leftNode) {
		this.leftNode = leftNode;
	}

	public Node getRightNode() {
		return rightNode;
	}

	public void setRightNode(Node rightNode) {
		this.rightNode = rightNode;
	}

	public Hyphen getHyphen() {
		return hyphen;
	}

	public void setHyphen(Hyphen hyphen) {
		this.hyphen = hyphen;
	}

	
    
    
    
//    public FilterExpression getLeftFilterExpression();
//    
//    
//    public FilterExpression getRightFilterExpression();
    
    
    
}
