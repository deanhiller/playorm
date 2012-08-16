package com.alvazan.orm.api.spi3.meta;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;
import com.alvazan.orm.api.spi9.db.ColumnType;

public enum StorageTypeEnum {

	STRING("String", String.class), DECIMAL("Decimal", BigDecimal.class), INTEGER("Integer", Integer.class), 
	BYTES("Bytes", byte[].class);
	
	private static Map<String, StorageTypeEnum> dbCodeToVal = new HashMap<String, StorageTypeEnum>();
	static {
		for(StorageTypeEnum type : StorageTypeEnum.values()) {
			dbCodeToVal.put(type.getDbValue(), type);
		}
	}
	
	private String dbValue;
	@SuppressWarnings("rawtypes")
	private Class javaType;

	@SuppressWarnings("rawtypes")
	private StorageTypeEnum(String dbValue, Class clazz) {
		this.dbValue = dbValue;
		this.javaType = clazz;
	}

	public String getDbValue() {
		return dbValue;
	}
	
	public static StorageTypeEnum lookupValue(String dbCode) {
		return dbCodeToVal.get(dbCode);
	}

	public String getIndexTableName() {
		return dbValue+"Indice";
	}

	public ColumnType translateStoreToColumnType() {
		switch (this) {
		case DECIMAL:
			return ColumnType.COMPOSITE_DECIMALPREFIX;
		case INTEGER:
			return ColumnType.COMPOSITE_INTEGERPREFIX;
		case STRING:
			return ColumnType.COMPOSITE_STRINGPREFIX;
		case BYTES:
			throw new UnsupportedOperationException("not sure if we need this one or not yet");
		default:
			throw new UnsupportedOperationException("We don't translate type="+this+" just yet");
		}
	}

	@SuppressWarnings("rawtypes")
	public Class getJavaType() {
		return javaType;
	}
	
	@SuppressWarnings("unchecked")
	public Object convertFromNoSql(byte[] data) {
		return StandardConverters.convertFromBytes(this.javaType, data);
	}
	public byte[] convertToNoSql(Object o) {
		return StandardConverters.convertToBytes(o);
	}
}
