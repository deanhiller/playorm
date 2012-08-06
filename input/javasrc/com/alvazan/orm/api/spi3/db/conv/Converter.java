package com.alvazan.orm.api.spi3.db.conv;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

}
