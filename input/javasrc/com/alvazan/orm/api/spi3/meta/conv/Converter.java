package com.alvazan.orm.api.spi3.meta.conv;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

	Object convertStringToType(String value);

	String convertTypeToString(Object dbValue);
}
