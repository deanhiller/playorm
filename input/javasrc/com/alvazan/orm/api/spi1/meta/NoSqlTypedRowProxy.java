package com.alvazan.orm.api.spi1.meta;

import java.util.Map;


public interface NoSqlTypedRowProxy {

	void __cacheIndexedValues();

	Map<String, Object> __getOriginalValues();

}
