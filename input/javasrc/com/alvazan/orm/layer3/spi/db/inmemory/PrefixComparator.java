package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.Comparator;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.layer3.spi.db.inmemory.IndexedRow.OurKey;

public class PrefixComparator implements Comparator<OurKey> {

	private Comparator<ByteArray> comparator;
	public PrefixComparator(Comparator<ByteArray> comparator) {
		this.comparator = comparator;
	}
	@Override
	public int compare(OurKey o1, OurKey o2) {
		int result = comparator.compare(o1.getPrefix(), o2.getPrefix());
		return result;
	}

}
