package com.alvazan.orm.api.spi2;

import java.util.Map;


public interface NoSqlTypedRowProxy {

	void __cacheIndexedValues();

	Map<String, Object> __getOriginalValues();

}
