package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.Comparator;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

public class Utf8Comparator implements Comparator<ByteArray> {

	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		String left = StandardConverters.convertFromBytes(String.class, o1.getKey());
		String right = StandardConverters.convertFromBytes(String.class, o2.getKey());
		return left.compareTo(right);
	}

}
