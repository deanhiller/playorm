package com.alvazan.orm.api.spi1.meta.conv;

/**
 * Any adhoc tool is string queries to convert to byte[] of nosql OR the opposite so this is the interface
 * for when string to byte[] or byte[] to string is necessary
 * @author dhiller2
 *
 */
public interface AdhocToolConverter {

	Object convertStringToType(String value);

	String convertTypeToString(Object dbValue);

}
