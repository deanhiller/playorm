package com.alvazan.orm.api.spi1.meta.conv;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

}
