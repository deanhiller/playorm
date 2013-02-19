package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.Map;


public interface NoSqlProxy {

	void __markInitializedAndCacheIndexedValues();

	Map<Field, Object> __getOriginalValues();

}
