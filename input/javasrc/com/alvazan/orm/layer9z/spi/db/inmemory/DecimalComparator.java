package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.math.BigDecimal;
import java.util.Comparator;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;

public class DecimalComparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		if(o1.getKey() == null && o2.getKey() != null)
			return -1;
		else if(o2.getKey() == null && o1.getKey() != null)
			return 1;
		else if(o2.getKey() == null && o1.getKey() == null)
			return 0;
		
		BigDecimal left = StandardConverters.convertFromBytes(BigDecimal.class, o1.getKey());
		BigDecimal right = StandardConverters.convertFromBytes(BigDecimal.class, o2.getKey());
		return left.compareTo(right);
	}

}
