package com.alvazan.orm.api;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

}
