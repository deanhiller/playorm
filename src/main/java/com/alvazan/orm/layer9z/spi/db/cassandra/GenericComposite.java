package com.alvazan.orm.layer9z.spi.db.cassandra;

import com.netflix.astyanax.annotations.Component;

public class GenericComposite {

	@Component(ordinal=0)
	private	byte[] indexedValue;
	@Component(ordinal=1)
	private	byte[] pk;
	
	byte[] getIndexedValue() {
		return indexedValue;
	}
	void setIndexedValue(byte[] value) {
		this.indexedValue = value;
	}
	byte[] getPk() {
		return pk;
	}
	void setPk(byte[] pk) {
		this.pk = pk;
	}
	
}
