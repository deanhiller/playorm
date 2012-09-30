package com.alvazan.orm.api.z8spi.meta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class TypedColumn {
	private String name;
	//for composite columns
	private String subName;
	
	private Object value;
	private Long timestamp;
	private DboColumnMeta columnMeta;

	public TypedColumn(DboColumnMeta colMeta, String name, String subName, Object value, Long timestamp2) {
		this(colMeta, name, value, timestamp2);
		this.subName = subName;
	}
	
	public TypedColumn(DboColumnMeta colMeta, String name, Object value, Long timestamp2) {
		this.columnMeta = colMeta;
		this.name = name;
		this.value = value;
		this.timestamp = timestamp2;
	}
	public TypedColumn(DboColumnMeta colMeta) {
		this.columnMeta = colMeta;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getValueAsString() {
		return columnMeta.convertTypeToString(value);
	}
	public BigDecimal getValueAsBigDecimal() {
		return (BigDecimal) value;
	}
	public BigInteger getValueAsBigInteger() {
		return (BigInteger) value;
	}
	@SuppressWarnings("rawtypes")
	public List getValueAsList() {
		return (List) value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public void setValueStr(String val) {
		this.value = columnMeta.convertStringToType(val);
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public Object getValue() {
		return value;
	}

	public String getCompositeSubName() {
		return subName;
	}

	public byte[] getRawValue() {
		return columnMeta.convertToStorage2(value);
	}
}
