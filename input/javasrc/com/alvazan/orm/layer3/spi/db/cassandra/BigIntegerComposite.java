package com.alvazan.orm.layer3.spi.db.cassandra;

import java.math.BigInteger;

import com.netflix.astyanax.annotations.Component;

//Need to get rid of this and do raw bytes so translation is not needed back and forth(That extra translation is annoying)
@Deprecated
public class BigIntegerComposite {

	@Component(ordinal=0)
	BigInteger value;
	@Component(ordinal=1)
	byte[] pk;
	
}
