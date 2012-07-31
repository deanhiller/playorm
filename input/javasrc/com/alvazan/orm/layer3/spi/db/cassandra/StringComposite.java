package com.alvazan.orm.layer3.spi.db.cassandra;

import com.netflix.astyanax.annotations.Component;

public class StringComposite {

	@Component(ordinal=0)
	String value;
	@Component(ordinal=1)
	byte[] pk;
	
}
