package com.alvazan.orm.layer3.spi.db.cassandra;

import com.netflix.astyanax.annotations.Component;

public class BigDecimalComposite {

	@Component(ordinal=0)
	byte[] value;
	@Component(ordinal=1)
	byte[] pk;
	
}
