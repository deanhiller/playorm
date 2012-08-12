package com.alvazan.orm.api.spi2;

import java.lang.reflect.Field;
import java.util.Map;


public interface NoSqlTypedRowProxy {

	void __markInitializedAndCacheIndexedValues();

	Map<Field, Object> __getOriginalValues();

}
