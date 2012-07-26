package com.alvazan.orm.layer3.spi.db.inmemory;

import java.math.BigInteger;
import java.util.Comparator;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

public class IntegerComparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		BigInteger left = StandardConverters.convertFromBytes(BigInteger.class, o1.getKey());
		BigInteger right = StandardConverters.convertFromBytes(BigInteger.class, o2.getKey());
		return left.compareTo(right);
	}

}
