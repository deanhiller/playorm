package com.alvazan.orm.api.spi2;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class TypedColumn {
	private String name;
	private Object value;
	private Long timestamp;
	
	public TypedColumn() {}
	public TypedColumn(String name, Object value) {
		this.name = name;
		this.value = value;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getValueAsString() {
		return (String) value;
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
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public Object getValue() {
		return value;
	}
}
