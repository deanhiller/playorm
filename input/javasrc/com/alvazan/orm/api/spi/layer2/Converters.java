package com.alvazan.orm.api.spi.layer2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


public class Converters {

	public static final StringConverter STRING_CONVERTER = new StringConverter();
	public static final BooleanConverter BOOLEAN_CONVERTER = new BooleanConverter();
	public static final ShortConverter SHORT_CONVERTER = new ShortConverter();
	public static final IntConverter INT_CONVERTER = new IntConverter();
	public static final LongConverter LONG_CONVERTER = new LongConverter();
	public static final FloatConverter FLOAT_CONVERTER = new FloatConverter();
	public static final DoubleConverter DOUBLE_CONVERTER = new DoubleConverter();
	public static final ByteConverter BYTE_CONVERTER = new ByteConverter();
	public static final ByteArrayConverter BYTE_ARRAY_CONVERTER = new ByteArrayConverter();
	public static final BaseConverter BIGINTEGER_CONVERTER = new BigIntegerConverter();
	
	public static class BigIntegerConverter extends BaseConverter {
		@Override
		public byte[] convertToNoSql(Object value) {
			if(value == null)
				return null;
			BigInteger val = (BigInteger)value;
			return val.toByteArray();
		}

		@Override
		public Object convertFromNoSql(byte[] value) {
			return new BigInteger(value);
		}

		@Override
		public byte[] convertToNoSqlFromString(String value) {
			return convertToNoSql(new BigInteger(value));
		}
		
	}
	
	public static class ByteArrayConverter extends BaseConverter {

		@Override
		public byte[] convertToNoSql(Object value) {
			return (byte[])value;
		}

		@Override
		public Object convertFromNoSql(byte[] value) {
			return value;
		}
		
		public byte[] convertToNoSqlFromString(String value) {
			try {
				return Hex.decodeHex(value.toCharArray());
			} catch (DecoderException e) {
				throw new RuntimeException(e);
			}
		}
		
		public String convertFromNoSqlToString(byte[] dbValue) {
			return new String(Hex.encodeHex(dbValue));
		}
		
		@Override
		public String convertToIndexFormat(Object value) {
			return convertFromNoSqlToString((byte[]) value);
		}

	}
	
	public static abstract class BaseConverter implements Converter, AdhocToolConverter {
		public String convertFromNoSqlToString(byte[] dbValue) {
			return convertFromNoSql(dbValue)+"";
		}
		
		@Override
		public String convertToIndexFormat(Object value) {
			if(value == null)
				return null;
			return value+"";
		}
	}
	
	public static abstract class AbstractConverter extends BaseConverter {
		
		public byte[] convertToNoSqlFromString(String value) {
			if(value == null)
				return null;
			
			Object typedValue = convertToPrimitive(value);
			return convertToNoSql(typedValue);
		}
		
		protected abstract Object convertToPrimitive(String value);

		@Override
		public byte[] convertToNoSql(Object value) {
			try {
				if(value == null)
					return null;
				ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputStream(outBytes);
				write(out, value);
				return outBytes.toByteArray();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		protected abstract void write(DataOutputStream out, Object value) throws IOException;
		
		@Override
		public Object convertFromNoSql(byte[] bytes) {
			try {
				if(bytes == null)
					return null;
				ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
				DataInputStream in = new DataInputStream(byteIn);
				return read(in);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		protected abstract Object read(DataInputStream in) throws IOException;

	}
	
	public static class StringConverter extends BaseConverter {

		public byte[] convertToNoSqlFromString(String value) {
			if(value == null)
				return null;
			
			return convertToNoSql(value);
		}
		
		@Override
		public byte[] convertToNoSql(Object value) {
			try {
				if(value == null)
					return null;
				String temp = (String) value;
				return temp.getBytes("UTF8");
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public Object convertFromNoSql(byte[] bytes) {
			try {
				if(bytes == null)
					return null;
				return new String(bytes, "UTF8");
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static class ShortConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value) throws IOException {
			out.writeShort((Short) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readShort();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Short.parseShort(value);
		}
	}
	
	public static class IntConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value) throws IOException {
			out.writeInt((Integer) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readInt();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Integer.parseInt(value);
		}
	}

	public static class LongConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value) throws IOException {
			out.writeLong((Long) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readLong();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Long.parseLong(value);
		}
	}
	
	public static class FloatConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value) throws IOException {
			out.writeFloat((Float) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readFloat();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Float.parseFloat(value);
		}
	}
	
	public static class DoubleConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value)
				throws IOException {
			out.writeDouble((Double) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readDouble();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Double.parseDouble(value);
		}
	}
	
	public static class BooleanConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value)
				throws IOException {
			out.writeBoolean((Boolean) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readBoolean();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Boolean.parseBoolean(value);
		}
	}
	
	public static class ByteConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value)
				throws IOException {
			out.writeByte((Integer) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readByte();
		}
		@Override
		protected Object convertToPrimitive(String value) {
			return Byte.parseByte(value);
		}
	}
	
}
