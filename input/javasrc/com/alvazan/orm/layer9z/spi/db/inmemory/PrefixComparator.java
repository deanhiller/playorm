package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.Comparator;

import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.layer9z.spi.db.inmemory.IndexedRow.OurKey;

public class PrefixComparator implements Comparator<OurKey> {

	private Comparator<ByteArray> comparator;
	public PrefixComparator(Comparator<ByteArray> comparator) {
		this.comparator = comparator;
	}
	@Override
	public int compare(OurKey o1, OurKey o2) {
		int result = comparator.compare(o1.getPrefix(), o2.getPrefix());
		if(result != 0)
			return result;
		
		//If prefixes are equal, we still need to sort them and order doesn't really matter so we will sort by
		//the postfix ByteArray
		
		//If postfix is null, it is greater than everything and should ONLY be used when querying anyways!!!
		if(o2.getPostfix() == null)
			return -1;
		else if(o1.getPostfix() == null)
			return 1;
		
		return o1.getPostfix().compareTo(o2.getPostfix());
	}

}
