package com.alvazan.orm.api.spi2;

public enum TypeEnum {

	STRING("StringIndice"), DECIMAL("DecimalIndice"), INTEGER("IntegerIndice"), BYTES(null);
	
	private String indexTableName;

	private TypeEnum(String indexTableName) {
		this.indexTableName = indexTableName;
	}

	public String getIndexTableName() {
		return indexTableName;
	}
	
}
