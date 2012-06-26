package com.alvazan.orm.parser.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Huai Jiang
 *
 */
public class SelectClause implements Node{

	private List<Attribute> projections = new ArrayList<Attribute>();
	
	public void addProjection(Attribute column) {
		projections.add(column);
	}
	
	public List<Attribute> getProjections() {
		return projections;
	}
	
}
