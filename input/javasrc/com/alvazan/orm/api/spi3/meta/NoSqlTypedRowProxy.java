package com.alvazan.orm.api.spi3.meta;

import java.util.Map;


public interface NoSqlTypedRowProxy {

	void __cacheIndexedValues();

	Map<String, Object> __getOriginalValues();

}
