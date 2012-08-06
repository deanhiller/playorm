package com.alvazan.orm.layer3.spi.db.cassandra;

import com.netflix.astyanax.annotations.Component;

public class GenericComposite {

	@Component(ordinal=0)
	private	byte[] value;
	@Component(ordinal=1)
	private	byte[] pk;
	
	byte[] getValue() {
		return value;
	}
	void setValue(byte[] value) {
		this.value = value;
	}
	byte[] getPk() {
		return pk;
	}
	void setPk(byte[] pk) {
		this.pk = pk;
	}
	
}
