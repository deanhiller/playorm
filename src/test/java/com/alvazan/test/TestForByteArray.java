package com.alvazan.test;

import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.layer9z.spi.db.inmemory.RowImpl;
import com.alvazan.orm.layer9z.spi.db.inmemory.Utf8Comparator;

public class TestForByteArray {

	@Test
	public void basicPutTest() {

		TreeMap<ByteArray, Column> map = new TreeMap<ByteArray, Column>(new Utf8Comparator());
		RowImpl row = new RowImpl(map);
		
		Column c1 = creteColumn("6e756d526f6f6d73", (byte) 1);
		Column c2 = creteColumn("6275696c64696e67", (byte) 2);
		Column c3 = creteColumn("6e616d65", (byte) 3);
		Column c4 = creteColumn("5f686973746f72790000013caaa1b98d5f7374617465", (byte) 4);
		Column c5 = creteColumn("5f686973746f72790000013caaa1b98d6275696c64696e67", (byte) 5);
		Column c6 = creteColumn("5f686973746f72790000013caaa1b98d6e756d526f6f6d73", (byte) 6);
		Column c7 = creteColumn("5f686973746f72790000013caaa1b9ba6275696c64696e67", (byte) 7);
		Column c8 = creteColumn("5f686973746f72790000013caaa1b9ba6e756d526f6f6d73", (byte) 8);
		
		row.put(c1);
		Assert.assertEquals(1, row.getColumns().size());
		row.put(c2);
		Assert.assertEquals(2, row.getColumns().size());
		row.put(c3);
		Assert.assertEquals(3, row.getColumns().size());
		row.put(c4);
		Assert.assertEquals(4, row.getColumns().size());
		row.put(c5);
		Assert.assertEquals(5, row.getColumns().size());
		row.put(c6);
		Assert.assertEquals(6, row.getColumns().size());
		row.put(c7);
		Assert.assertEquals(7, row.getColumns().size());
		row.put(c8);
		Assert.assertEquals(8, row.getColumns().size());
	}

	private Column creteColumn(String hex, byte i) {
		byte[] name = StandardConverters.convertFromString(byte[].class, hex);
		byte[] value = new byte[] { i };
		Column c = new Column(name, value);
		return c;
	}
}
