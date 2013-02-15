package com.alvazan.orm.layer0.base;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * All because darn apache commons BeanUtils copies by method which breaks for some clients when they don't have getters/setters AND they
 * should not have to have getters/setters
 * @author dhiller2
 *
 */
@SuppressWarnings("rawtypes")
public class BeanProps {

	public static void copyProps(Object src, Object dest) {
		try {
			copyPropsImpl(src, dest);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void copyPropsImpl(Object src, Object dest) throws IllegalAccessException {
		Class<? extends Object> clazz = src.getClass();
		List<Field> fields = findAllFields(clazz);
		
		for(Field f : fields) {
			f.setAccessible(true);
			if (!Modifier.isFinal(f.getModifiers())) {
				Object value = f.get(src);
				f.set(dest, value);
			}
		}
	}
	
	private static List<Field> findAllFields(Class<?> metaClass) {
		List<Field[]> fields = new ArrayList<Field[]>();
		findFields(metaClass, fields);
		
		List<Field> allFields = new ArrayList<Field>();
		for(Field[] f : fields) {
			List<Field> asList = Arrays.asList(f);
			allFields.addAll(asList);
		}
		return allFields;
	}
	
	private static void findFields(Class metaClass2, List<Field[]> fields) {
		Class next = metaClass2;
		while(true) {
			Field[] f = next.getDeclaredFields();
			fields.add(f);
			next = next.getSuperclass();
			if(next.equals(Object.class))
				return;
		}
	}
}
