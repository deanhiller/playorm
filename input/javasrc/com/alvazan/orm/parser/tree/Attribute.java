package com.alvazan.orm.parser.tree;

public class Attribute implements Node{
	
	
	private String attributeName ;
	
	private String entityName;
	
	
	public Attribute(String entityName, String attributeName) {
		this.entityName = entityName;
		this.attributeName = attributeName;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	
	@Override
	public String toString() {
		return "AttributeName:["+this.attributeName+"] from Entity ["+entityName+"]";
	}

}
