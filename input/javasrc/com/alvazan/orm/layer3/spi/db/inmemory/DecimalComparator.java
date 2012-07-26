package com.alvazan.orm.layer3.spi.db.inmemory;

import java.math.BigDecimal;
import java.util.Comparator;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

public class DecimalComparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		BigDecimal left = StandardConverters.convertFromBytes(BigDecimal.class, o1.getKey());
		BigDecimal right = StandardConverters.convertFromBytes(BigDecimal.class, o2.getKey());
		return left.compareTo(right);
	}

}
