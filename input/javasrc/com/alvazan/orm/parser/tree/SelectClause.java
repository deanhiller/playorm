package com.alvazan.orm.parser.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Huai Jiang
 *
 */
public class SelectClause implements Node{

	private List<String> projections = new ArrayList<String>();
	
	public void addProjection(String column) {
		projections.add(column);
	}
	
	public List<String> getProjections() {
		return projections;
	}
	
}
