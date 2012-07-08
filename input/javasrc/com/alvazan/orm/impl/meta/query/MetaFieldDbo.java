package com.alvazan.orm.impl.meta.query;

public class MetaFieldDbo {

	private String name;
	/**
	 * null for FK relationships
	 */
	private String columnType;
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	private MetaClassDbo fkToColumnFamily;
	
	private boolean isToManyColumn;
	
	public String getName() {
		return name;
	}

	public void setName(String field) {
		this.name = field;
	}

	@Override
	public String toString() {
		return "Field["+name+"]";
	}

	public void setup(String colName, MetaClassDbo fkToTable, String classType, boolean isToManyColumn) {	
		this.name = colName;
		this.fkToColumnFamily = fkToTable;
		this.columnType = classType;
		this.isToManyColumn = isToManyColumn;
	}

	@SuppressWarnings("rawtypes")
	public Class getClassType() {
		try {
			return Class.forName(columnType);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
}
