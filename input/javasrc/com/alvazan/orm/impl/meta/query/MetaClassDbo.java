package com.alvazan.orm.impl.meta.query;

import java.util.HashMap;
import java.util.Map;

public class MetaClassDbo {

	private String columnFamily;
	private Map<String, MetaFieldDbo> nameToField = new HashMap<String, MetaFieldDbo>();

	public void setTableName(String tableName) {
		this.columnFamily = tableName;
	}
	
	public String getTableName() {
		return columnFamily;
	}

	public void addField(MetaFieldDbo fieldDbo) {
		nameToField.put(fieldDbo.getName(), fieldDbo);
	}
	
	public MetaFieldDbo getMetaField(String attributeName) {
		return nameToField.get(attributeName);
	}
	
	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+"]";
	}

}