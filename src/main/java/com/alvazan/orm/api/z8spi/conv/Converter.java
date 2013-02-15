package com.alvazan.orm.api.z8spi.conv;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

	Object convertStringToType(String value);

	String convertTypeToString(Object dbValue);
}
