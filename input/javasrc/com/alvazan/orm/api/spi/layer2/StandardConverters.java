package com.alvazan.orm.api.spi.layer2;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.layer2.Converters.BaseConverter;

@SuppressWarnings("rawtypes")
public class StandardConverters {

	private static Map<Class, BaseConverter> stdConverters = new HashMap<Class, BaseConverter>();
	
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
		stdConverters.put(BigInteger.class, Converters.BIGINTEGER_CONVERTER);
	}

	public static BaseConverter get(Class type) {
		return stdConverters.get(type);
	}

	public static boolean containsConverterFor(Class newType) {
		return stdConverters.containsKey(newType);
	}
}
