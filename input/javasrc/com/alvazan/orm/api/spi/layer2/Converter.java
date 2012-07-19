package com.alvazan.orm.api.spi.layer2;

public interface Converter {

	byte[] convertToNoSql(Object value);

	Object convertFromNoSql(byte[] value);

	/**
	 * If isIndexingSupported is true, you must implement this method.  If false, you can throw unsupported operationException
	 * with this method as we will never call it
	 */
	String convertToIndexFormat(Object value);
}
