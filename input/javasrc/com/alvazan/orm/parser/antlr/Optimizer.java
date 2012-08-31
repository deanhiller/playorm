package com.alvazan.orm.parser.antlr;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Optimizer {

	private final static Logger log = LoggerFactory.getLogger(Optimizer.class);
	
	public ParsedNode optimize(ParsedNode node, MetaFacade facade, String query) {
		if(node == null)
			return null;
		
		ParsedNode root = optimizeGtLtToBetween(node, query, facade);
		
		root = addJoinInformation(root);
		
		return root;
	}

	private ParsedNode addJoinInformation(ParsedNode root) {
		OptimizeAddJoinInfo treeWalker = new OptimizeAddJoinInfo();
		return treeWalker.walkTree(root);
	}

	private ParsedNode optimizeGtLtToBetween(ParsedNode node, String query, MetaFacade facade) {
		Map<String, Integer> attrs = facade.getAttributeUsedCount();
		ParsedNode root = node;
		for(Entry<String, Integer> m : attrs.entrySet()) {
			if(m.getValue().intValue() <= 1)
				continue;
			
			log.info("optimizing query tree for varname="+m.getKey());
			OptimizeGltLtConversion visitor = new OptimizeGltLtConversion(m.getKey());
			root = visitor.walkAndFixTree(root, query, facade);
		}
		return root;
	}
	
//	String msg = "this type="+this.getType()+" node type="+attrExpNode.getType();
//	ExpressionNode attributeSideNode = (ExpressionNode) attrExpNode.getChild(ChildSide.LEFT);
//	StateAttribute state2 = (StateAttribute) attributeSideNode.getState();
//	if(attrExpNode.getType() == NoSqlLexer.EQ || this.getType() == NoSqlLexer.EQ) {
//		throw new IllegalArgumentException("uhhhmmmm, you are using column="+state2.getColumnInfo().getColumnName()
//				+" twice with 'AND' statement yet one has an = so change to 'OR' or get rid of one. "+ msg);
//	} else if(attrExpNode.getType() == NoSqlLexer.GE || attrExpNode.getType() == NoSqlLexer.GT) {
//		greaterThanExpression = (ExpressionNode) attrExpNode;
//		lessThanExpression = this;
//	} else if(this.getType() == NoSqlLexer.GE || this.getType() == NoSqlLexer.GT) {
//		greaterThanExpression = this;
//		lessThanExpression = (ExpressionNode) attrExpNode;
//	} else if(this.getType() == NoSqlLexer.LE || this.getType() == NoSqlLexer.LT) {
//		throw new IllegalArgumentException("uhmmmm, you are using column="+state2.getColumnInfo()+" twice(which is fine) but both of them are using greater than, delete one of them. " +msg);
//	} else
//		throw new RuntimeException("bug, we should never get here but this should be easy to fix.  "+msg);
}
