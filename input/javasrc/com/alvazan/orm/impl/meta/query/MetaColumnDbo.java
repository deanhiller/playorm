package com.alvazan.orm.impl.meta.query;

public class MetaColumnDbo {

	private String columnName;
	/**
	 * null for FK relationships
	 */
	private String columnType;
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	private MetaTableDbo fkToColumnFamily;
	
	private boolean isToManyColumn;
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String field) {
		this.columnName = field;
	}

	@Override
	public String toString() {
		return "Field["+columnName+"]";
	}

	public void setup(String colName, MetaTableDbo fkToTable, String classType, boolean isToManyColumn) {	
		this.columnName = colName;
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
