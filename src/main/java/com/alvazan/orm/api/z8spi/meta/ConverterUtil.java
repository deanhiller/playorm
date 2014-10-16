package com.alvazan.orm.api.z8spi.meta;

import java.util.HashMap;

import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
public class ConverterUtil {
	protected static HashMap<String, Class> loadedClasses = new HashMap<String, Class>();

	public static StorageTypeEnum getStorageType(Class fieldType) {
		StorageTypeEnum type = StandardConverters.getStorageType(fieldType);
		if(type == null)
			return StorageTypeEnum.BYTES;
		return type;
	}

	protected static Class classForName(String columnType) {
		try {
			Class c = loadedClasses.get(columnType);
			if (c == null) {
				c = Class.forName(columnType);
				loadedClasses.put(columnType, c);
			}
			return c;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected static Class translateType(Class classType) {
		return convertIfPrimitive(classType);
	}

	public static Class convertIfPrimitive(Class fieldType) {
		Class c = fieldType;
		if (long.class.equals(fieldType))
			c = Long.class;
		else if (int.class.equals(fieldType))
			c = Integer.class;
		else if (short.class.equals(fieldType))
			c = Short.class;
		else if (byte.class.equals(fieldType))
			c = Byte.class;
		else if (double.class.equals(fieldType))
			c = Double.class;
		else if (float.class.equals(fieldType))
			c = Float.class;
		else if (boolean.class.equals(fieldType))
			c = Boolean.class;
		else if (char.class.equals(fieldType))
			c = Character.class;
		return c;
	}

}
