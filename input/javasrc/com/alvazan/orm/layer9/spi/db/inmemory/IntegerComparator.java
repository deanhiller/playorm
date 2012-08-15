package com.alvazan.orm.layer9.spi.db.inmemory;

import java.math.BigInteger;
import java.util.Comparator;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;

public class IntegerComparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		if(o1.getKey() == null && o2.getKey() != null)
			return -1;
		else if(o2.getKey() == null && o1.getKey() != null)
			return 1;
		else if(o2.getKey() == null && o1.getKey() == null)
			return 0;
		
		BigInteger left = StandardConverters.convertFromBytes(BigInteger.class, o1.getKey());
		BigInteger right = StandardConverters.convertFromBytes(BigInteger.class, o2.getKey());
		
		return left.compareTo(right);
	}

}
