package com.alvazan.orm.api.spi2;

import java.lang.reflect.Field;

public class ReflectionUtil {

	public static <T> T create(Class<T> c) {
		try {
			return c.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void putFieldValue(Object entity, Field field, Object value) {
		try {
			//NOTE: There was some kind of major bug where if we run field.set on a javassist.util.Proxy object
			// and when we set field entity.id, not only
			//did entity.id get modified, BUT entity.nameToValue ALSO was modified for some dang
			//reason and I am not sure why, so NOW instead of using field.set on javassist.util.Proxy
			//we are going to call a special method on the Proxy to try to set the field instead
			
			field.set(entity, value);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static Object fetchFieldValue(Object entity, Field field) {
		try {
			return field.get(entity);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}	
}
