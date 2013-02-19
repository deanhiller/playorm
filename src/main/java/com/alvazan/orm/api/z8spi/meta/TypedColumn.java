package com.alvazan.orm.api.z8spi.meta;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.alvazan.orm.api.z8spi.conv.StandardConverters;

public class TypedColumn {
	private byte[] name;
	//for composite columns
	private byte[] subName;
	
	private byte[] value;
	private Long timestamp;
	private DboColumnMeta columnMeta;

	public TypedColumn(DboColumnToManyMeta colMeta, byte[] prefixName, byte[] postFixName, byte[] value, Long timestamp2) {
		this(colMeta, prefixName, value, timestamp2);
		this.subName = postFixName;
	}

	public TypedColumn(DboColumnEmbedSimpleMeta colMeta, byte[] prefixName, byte[] postFixName, byte[] value, Long timestamp2) {
		this(colMeta, prefixName, value, timestamp2);
		this.subName = postFixName;
	}

	public TypedColumn(DboColumnToOneMeta colMeta, byte[] prefixName, byte[] postFixName, byte[] value, Long timestamp2) {
		this(colMeta, prefixName, value, timestamp2);
		this.subName = postFixName;
	}

	public TypedColumn(DboColumnEmbedMeta colMeta, byte[] prefixName, byte[] postFixName, byte[] value, Long timestamp2 ) {
		this(colMeta, prefixName, value, timestamp2);
		this.subName = postFixName;
	}

	public TypedColumn(DboColumnMeta colMeta, byte[] name, byte[] value, Long timestamp2) {
		this.columnMeta = colMeta;
		this.name = name;
		this.value = value;
		this.timestamp = timestamp2;
	}
	
	public byte[] getNameRaw() {
		if(subName == null)
			return name;
		
		byte[] all = new byte[name.length+subName.length];
		for(int i = 0; i < name.length;i++) {
			all[i] = name[i];
		}
		
		for(int i = 0; i < subName.length; i++) {
			all[i+name.length] = subName[i];
		}
		return all;
	}
	
	public String getName() {
		if(columnMeta == null)
			throw new IllegalArgumentException("You need to call getName(Class type) instead as this column is not defined in our schema");

		String strName = StandardConverters.convertFromBytes(String.class, name);
		if(!(columnMeta instanceof DboColumnToManyMeta || columnMeta instanceof DboColumnEmbedSimpleMeta || columnMeta instanceof DboColumnToOneMeta || columnMeta instanceof DboColumnEmbedMeta ))
			return strName;
		if (columnMeta instanceof DboColumnToManyMeta) {
			DboColumnToManyMeta many = (DboColumnToManyMeta) columnMeta;
			if (subName !=null) {
				//Just if the subName is null and use in some other way like in DeleteColumn
				Object objVal = many.convertFromStorage2(subName);
				return strName+"."+many.convertTypeToString(objVal);
			}
			else
				return strName;
		} else if (columnMeta instanceof DboColumnToOneMeta) {
			DboColumnToOneMeta one = (DboColumnToOneMeta) columnMeta;
			if (subName !=null) {
				//Just if it is stored as composite
				Object objVal = one.convertFromStorage2(subName);
				return strName+"."+one.convertTypeToString(objVal);	
			}
			else
				return strName;
		} else if (columnMeta instanceof DboColumnEmbedSimpleMeta) {
			DboColumnEmbedSimpleMeta embedSimple = (DboColumnEmbedSimpleMeta) columnMeta;
			Object objVal = embedSimple.convertFromStorage2(subName);
			return strName+"."+embedSimple.convertTypeToString(objVal);

		} else if (columnMeta instanceof DboColumnEmbedMeta) {
			DboColumnEmbedMeta embedMeta = (DboColumnEmbedMeta) columnMeta;
			Object objVal = embedMeta.convertFromStorage2(subName);
			return strName + "." + embedMeta.convertTypeToString(objVal);
		}
		// in any other case return strName
		return strName;
	}
	
	public String getNameAsString(Class<?> type) {
		if(columnMeta != null)
			throw new IllegalArgumentException("This is defined in schema, call getName() instead");
		
		Object data = StandardConverters.convertFromBytes(type, name);
		return StandardConverters.convertToString(data);
	}

	public <T> T getNameAsType(Class<T> type) {
		if(columnMeta != null)
			throw new IllegalArgumentException("This is defined in schema, call getName() instead");
		
		return StandardConverters.convertFromBytes(type, name);
	}
	
	public void setNameRaw(byte[] name) {
		this.name = name;
	}
	
	public void setName(Object name) {
		if(columnMeta != null) {
			this.name = StandardConverters.convertToBytes(name);
			return;
		}
		
		this.name = StandardConverters.convertToBytes(name);
	}
	
	public String getValueAsString() {
		if(columnMeta != null) {
			Object val = columnMeta.convertFromStorage2(value);
			return columnMeta.convertTypeToString(val);
		}
		
		return StandardConverters.convertToString(value);
	}
	
	public String getValueAsString(Class clazz) {
		Object obj = StandardConverters.convertFromBytes(clazz, value);
		return StandardConverters.convertToString(obj);
	}

	@Deprecated
	public BigDecimal getValueAsBigDecimal() {
		return StandardConverters.convertFromBytes(BigDecimal.class, value);
	}
	@Deprecated
	public BigInteger getValueAsBigInteger() {
		return StandardConverters.convertFromBytes(BigInteger.class, value);
	}
	
	public void setValue(Object value) {
		if(columnMeta != null) {
			this.value = columnMeta.convertToStorage2(value);
			return;
		}
		
		this.value = StandardConverters.convertToBytes(value);
	}
	
	public void setValueStr(String val) {
		if(columnMeta == null)
			throw new IllegalArgumentException("Call setValueStr(String val, Class type) instead as we don't know the type to store as");

		Object objVal = columnMeta.convertStringToType(val);
		this.value = columnMeta.convertToStorage2(objVal);
	}

	public void setValueStr(String val, Class<?> typeToStoreAs) {
		if(columnMeta != null)
			throw new IllegalArgumentException("Call setValueStr(String val) instead as we have the type information for this column");
		
		Object objVal = StandardConverters.convertFromString(typeToStoreAs, val);
		this.value = StandardConverters.convertToBytes(objVal);
	}
	
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public Object getValue() {
		if(columnMeta == null)
			throw new IllegalArgumentException("Call getValue(Class type) instead as this column is not in our schema");
		
		return columnMeta.convertFromStorage2(value);
	}

	public <T> T getValue(Class<T> asType) {
		return StandardConverters.convertFromBytes(asType, value);
	}
	public byte[] getValueRaw() {
		return value;
	}
	
	public byte[] getCompositeSubName() {
		return subName;
	}

	public Boolean getValueAsBoolean() {
		return StandardConverters.convertFromBytes(Boolean.class, value);
	}

	public DboColumnMeta getColumnMeta() {
		return columnMeta;
	}

}
