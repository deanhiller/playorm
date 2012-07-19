package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.layer2.Converters.AbstractConverter;

public class StandardConverters {

	@SuppressWarnings("rawtypes")
	private static Map<Class, AbstractConverter> stdConverters = new HashMap<Class, AbstractConverter>();
	
	static {
		stdConverters.put(short.class, Converters.SHORT_CONVERTER);
		stdConverters.put(Short.class, Converters.SHORT_CONVERTER);
		stdConverters.put(int.class, Converters.INT_CONVERTER);
		stdConverters.put(Integer.class, Converters.INT_CONVERTER);
		stdConverters.put(long.class, Converters.LONG_CONVERTER);
		stdConverters.put(Long.class, Converters.LONG_CONVERTER);
		stdConverters.put(float.class, Converters.FLOAT_CONVERTER);
		stdConverters.put(Float.class, Converters.FLOAT_CONVERTER);
		stdConverters.put(double.class, Converters.DOUBLE_CONVERTER);
		stdConverters.put(Double.class, Converters.DOUBLE_CONVERTER);
		stdConverters.put(byte.class, Converters.BYTE_CONVERTER);
		stdConverters.put(Byte.class, Converters.BYTE_CONVERTER);
		stdConverters.put(boolean.class, Converters.BOOLEAN_CONVERTER);
		stdConverters.put(Boolean.class, Converters.BOOLEAN_CONVERTER);
		stdConverters.put(String.class, Converters.STRING_CONVERTER);		
		stdConverters.put(byte[].class, Converters.BYTE_ARRAY_CONVERTER);
	}

	public static AbstractConverter get(Class<?> type) {
		return stdConverters.get(type);
	}
}
