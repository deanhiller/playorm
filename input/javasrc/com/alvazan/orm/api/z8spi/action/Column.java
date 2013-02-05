package com.alvazan.orm.api.z8spi.action;

import com.alvazan.orm.api.z8spi.conv.ByteArray;

public class Column {
	private byte[] name;
	private byte[] value;
	private Long timestamp;

	public Column() {}
	
	public Column(byte[] name, byte[] value) {
		this.name = name;
		this.value = value;
	}
	
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	public byte[] getName() {
		return name;
	}
	public void setName(byte[] name) {
		this.name = name;
	}
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public Column copy() {
		//perhaps should deep copy here but we don't...
		Column c = new Column();
		c.name = name;
		c.timestamp = timestamp;
		//THIS MAKES us perform exactly like cassandra in that a 0 length byte or string is same as null!!!
		if(value != null && value.length != 0)
			c.value = value;
		return c;
	}

	@Override
	public String toString() {
		return "colName="+new ByteArray(name)+"/value="+new ByteArray(value);
	}
}
