package com.alvazan.orm.layer3.spi.db.cassandra;

import java.math.BigDecimal;

import com.netflix.astyanax.annotations.Component;

public class BigIntegerComposite {

	@Component(ordinal=0)
	BigDecimal value;
	@Component(ordinal=1)
	byte[] pk;
	
}
