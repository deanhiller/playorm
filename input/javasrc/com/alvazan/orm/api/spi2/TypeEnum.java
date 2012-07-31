package com.alvazan.orm.api.spi2;

import java.util.HashMap;
import java.util.Map;

public enum TypeEnum {

	STRING("StringIndice", "String"), DECIMAL("DecimalIndice", "Decimal"), INTEGER("IntegerIndice", "Integer"), BYTES(null, "Bytes");
	
	private static Map<String, TypeEnum> dbCodeToVal = new HashMap<String, TypeEnum>();
	static {
		for(TypeEnum type : TypeEnum.values()) {
			dbCodeToVal.put(type.getDbValue(), type);
		}
	}
	
	private String indexTableName;
	private String dbValue;

	private TypeEnum(String indexTableName, String dbValue) {
		this.indexTableName = indexTableName;
		this.dbValue = dbValue;
	}

	public String getIndexTableName() {
		return indexTableName;
	}

	public String getDbValue() {
		return dbValue;
	}
	
	public static TypeEnum lookupValue(String dbCode) {
		return dbCodeToVal.get(dbCode);
	}
	
}
