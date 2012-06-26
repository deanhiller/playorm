package com.alvazan.orm.parser.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Huai Jiang
 *
 */
public class FromClause implements Node {
	
	public static final String DEFAULTENTITY="defaultEntity";
	//TODO add alias mapping
	private List<String> entities = new ArrayList<String>();
	
	public void addEntity(String tableName) {
		this.entities.add(tableName);
		
	}
	public List<String> getEntities() {
		return entities;
	}

}
