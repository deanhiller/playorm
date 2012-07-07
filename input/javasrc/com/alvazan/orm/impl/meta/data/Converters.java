package com.alvazan.orm.impl.meta.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.alvazan.orm.api.base.Converter;

public class Converters {

	public static abstract class AbstractConverter implements Converter {
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
		
		@Override
		public boolean isIndexingSupported() {
			return false;
		}
		@Override
		public String convertToIndexFormat(Object value) {
			return null;
		}
	}
	
	public static class StringConverter extends AbstractConverter {
		@Override
		protected void write(DataOutputStream out, Object value)
				throws IOException {
			out.writeUTF((String) value);
		}
		@Override
		protected Object read(DataInputStream in) throws IOException {
			return in.readUTF();
		}
		@Override
		public boolean isIndexingSupported() {
			return true;
		}
		@Override
		public String convertToIndexFormat(Object value) {
			return (String) value;
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
		
	}
	
}
