package com.alvazan.orm.impl.meta.query;

import java.util.HashMap;
import java.util.Map;

public class MetaTableDbo {

	private String columnFamily;
	private Map<String, MetaColumnDbo> nameToField = new HashMap<String, MetaColumnDbo>();

	public void setTableName(String tableName) {
		this.columnFamily = tableName;
	}
	
	public String getTableName() {
		return columnFamily;
	}

	public void addField(MetaColumnDbo fieldDbo) {
		nameToField.put(fieldDbo.getColumnName(), fieldDbo);
	}
	
	public MetaColumnDbo getMetaField(String attributeName) {
		return nameToField.get(attributeName);
	}
	
	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+"]";
	}

}