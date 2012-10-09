package com.alvazan.orm.api.z8spi.conv;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

import com.alvazan.orm.api.z8spi.conv.Converters.BaseConverter;

@SuppressWarnings("rawtypes")
public class StandardConverters {

	private static Map<Class, BaseConverter> stdConverters = new HashMap<Class, BaseConverter>();
	private static Map<Class, StorageTypeEnum> storageTypes = new HashMap<Class, StorageTypeEnum>();
	
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
		stdConverters.put(BigDecimal.class, Converters.BIGDECIMAL_CONVERTER);
		stdConverters.put(DateTime.class, Converters.DATE_TIME);
		stdConverters.put(LocalDateTime.class, Converters.LOCAL_DATE_TIME);
		stdConverters.put(LocalDate.class, Converters.LOCAL_DATE);
		stdConverters.put(LocalTime.class, Converters.LOCAL_TIME);
		
		storageTypes.put(byte[].class, StorageTypeEnum.BYTES);
		storageTypes.put(Long.class, StorageTypeEnum.INTEGER);
		storageTypes.put(Integer.class, StorageTypeEnum.INTEGER);
		storageTypes.put(Short.class, StorageTypeEnum.INTEGER);
		storageTypes.put(Byte.class, StorageTypeEnum.INTEGER);
		storageTypes.put(Double.class, StorageTypeEnum.DECIMAL);
		storageTypes.put(Float.class, StorageTypeEnum.DECIMAL);
		storageTypes.put(Boolean.class, StorageTypeEnum.BOOLEAN);
		storageTypes.put(Character.class, StorageTypeEnum.STRING);
		storageTypes.put(String.class, StorageTypeEnum.STRING);
		storageTypes.put(BigDecimal.class, StorageTypeEnum.DECIMAL);
		storageTypes.put(BigInteger.class, StorageTypeEnum.INTEGER);
		storageTypes.put(LocalDateTime.class, StorageTypeEnum.INTEGER);
		storageTypes.put(LocalTime.class, StorageTypeEnum.INTEGER);
		storageTypes.put(LocalDate.class, StorageTypeEnum.INTEGER);
	}

	public static BaseConverter get(Class type) {
		return stdConverters.get(type);
	}

	public static boolean containsConverterFor(Class newType) {
		return stdConverters.containsKey(newType);
	}

	@SuppressWarnings("unchecked")
	public static <T> T convertFromBytes(Class<T> clazz, byte[] data) {
		if(data == null)
			return null;
		BaseConverter converter = stdConverters.get(clazz);
		if(converter == null)
			throw new IllegalArgumentException("Type not supported at this time="+clazz);		
		return (T) converter.convertFromNoSql(data);
	}
	
	/**
	 * Converts to BigInteger byte form OR BigDecimal byte form OR UTF8 byte form
	 * @param obj
	 * @return
	 */
	public static byte[] convertToBytes(Object obj) {
		Class clazz = obj.getClass();
		BaseConverter converter = stdConverters.get(clazz);
		if(converter == null)
			throw new IllegalArgumentException("Type clazz="+clazz+" is not supported at this time");
		return converter.convertToNoSql(obj);
	}
	
	/**
	 * Special method as if you convert 5 or 876, they convert to BigInteger byte form but here we
	 * specifically want BigDecimal byte form so we need to force that.
	 * @param obj
	 * @return
	 */
	public static byte[] convertToDecimalBytes(Object obj) {
		Object value = obj;
		Class clazz = obj.getClass();
		if(Byte.class.equals(clazz) 
				|| Short.class.equals(clazz)
				|| Integer.class.equals(clazz)
				|| Long.class.equals(clazz)) {
			clazz = Double.class;
			Double d = translate(obj);
			value = d;
		}
		
		BaseConverter converter = stdConverters.get(clazz);
		if(converter == null)
			throw new IllegalArgumentException("Type not supported at this time="+obj.getClass());
		return converter.convertToNoSql(value);
	}
	
	private static Double translate(Object obj) {
		if(obj == null)
			return null;

		double val;
		if(Byte.class.equals(obj.getClass())) {
			Byte b = (Byte)obj;
			val = b.byteValue();
		} else if(Short.class.equals(obj.getClass())) {
			Short s = (Short)obj;
			val = s.shortValue();
		} else if(Integer.class.equals(obj.getClass())) {
			Integer i = (Integer)obj;
			val = i.intValue();
		} else if(Long.class.equals(obj.getClass())) {
			Long l = (Long)obj;
			val = l.longValue();
		} else
			throw new RuntimeException("bug, should never get here");
		
		return val;
	}

	public static <T> T convertFromBytesNoExc(Class<T> clazz, byte[] data) {
		try {
			return convertFromBytes(clazz, data);
		} catch(Exception e) {
			return null;
		}
	}
	
	public static String convertToString(int i, Object data) {
		Class clazz = data.getClass();
		BaseConverter converter = stdConverters.get(clazz);
		if(converter == null)
			throw new IllegalArgumentException("Type clazz="+clazz+" is not supported at this time");
		return converter.convertToString(data);
	}
	
	public static Object convertFromString(Class<?> clazz, String data) {
		if(data == null)
			return null;
		BaseConverter converter = stdConverters.get(clazz);
		if(converter == null)
			throw new IllegalArgumentException("Type not supported at this time="+clazz);		
		return converter.convertStringToType(data);
	}
	
	public static StorageTypeEnum getStorageType(Class fieldType) {
		return storageTypes.get(fieldType);
	}

}
