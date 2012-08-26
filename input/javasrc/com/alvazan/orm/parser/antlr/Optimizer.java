package com.alvazan.orm.parser.antlr;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Optimizer {

	private final static Logger log = LoggerFactory.getLogger(Optimizer.class);
	
	public ParsedNode optimize(ParsedNode node,
			Map<String, Integer> attributeUsedCount, String query) {
		
		ParsedNode root = optimizeGtLtToBetween(node, attributeUsedCount, query);
		
		return root;
	}

	private ParsedNode optimizeGtLtToBetween(ParsedNode node,
			Map<String, Integer> attributeUsedCount, String query) {
		ParsedNode root = node;
		for(Entry<String, Integer> m : attributeUsedCount.entrySet()) {
			if(m.getValue().intValue() <= 1)
				continue;
			
			log.info("optimizing query tree for varname="+m.getKey());
			WalkTreeOptimizer visitor = new WalkTreeOptimizer(m.getKey());
			root = visitor.walkAndFixTree(root, query);
		}
		return root;
	}
}
