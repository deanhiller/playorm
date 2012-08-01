package com.alvazan.orm.layer3.spi.db.cassandra;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AbstractSerializer;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

	private static Serializer<BigDecimal> singleton = new BigDecimalSerializer();


	public static Serializer<BigDecimal> get() {
		return singleton;
	}


	@Override
	public BigDecimal fromByteBuffer(ByteBuffer arg0) {
		throw new UnsupportedOperationException("not supported yet");
	}


	@Override
	public ByteBuffer toByteBuffer(BigDecimal arg0) {
		throw new UnsupportedOperationException("not supported yet");
	}

}
