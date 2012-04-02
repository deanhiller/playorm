package com.alvazan.orm.impl.meta;

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
