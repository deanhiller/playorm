package com.alvazan.orm.parser.antlr;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer5.nosql.cache.InfoForWiring;

public class Optimizer {

	private final static Logger log = LoggerFactory.getLogger(Optimizer.class);
	
	public ParsedNode optimize(ParsedNode node,
			InfoForWiring wiring, String query) {
		
		ParsedNode root = optimizeGtLtToBetween(node, wiring, query);
		
		root = addJoinInformation(root, wiring);
		
		return root;
	}

	private ParsedNode addJoinInformation(ParsedNode root, InfoForWiring wiring) {
		AddJoinInfo treeWalker = new AddJoinInfo();
		return treeWalker.walkTree(root, wiring);
	}

	private ParsedNode optimizeGtLtToBetween(ParsedNode node,
			InfoForWiring wiring, String query) {
		Map<String, Integer> attrs = wiring.getAttributeUsedCount();
		ParsedNode root = node;
		for(Entry<String, Integer> m : attrs.entrySet()) {
			if(m.getValue().intValue() <= 1)
				continue;
			
			log.info("optimizing query tree for varname="+m.getKey());
			GltLtConvertToInBetween visitor = new GltLtConvertToInBetween(m.getKey());
			root = visitor.walkAndFixTree(root, query);
		}
		return root;
	}
}
