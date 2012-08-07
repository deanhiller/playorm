package com.alvazan.orm.api.spi2;

public enum ColumnTypeEnum {

	LIST_OF_FK("listOfFk"), FK("fk"), GENERIC("generic"), ID("id");
	
	private String dbCode;
	
	private ColumnTypeEnum(String code) {
		dbCode = code;
	}

	public String getDbCode() {
		return dbCode;
	}
	
}

