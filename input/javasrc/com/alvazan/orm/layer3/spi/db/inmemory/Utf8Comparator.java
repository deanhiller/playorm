package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.Comparator;

import com.alvazan.orm.api.spi1.meta.conv.ByteArray;
import com.alvazan.orm.api.spi1.meta.conv.StandardConverters;

public class Utf8Comparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		if(o1.getKey() == null && o2.getKey() != null)
			return -1;
		else if(o2.getKey() == null && o1.getKey() != null)
			return 1;
		else if(o2.getKey() == null && o1.getKey() == null)
			return 0;
		
		String left = StandardConverters.convertFromBytes(String.class, o1.getKey());
		String right = StandardConverters.convertFromBytes(String.class, o2.getKey());
		return left.compareTo(right);
	}

}
