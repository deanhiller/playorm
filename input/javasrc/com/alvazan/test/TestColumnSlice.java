package com.alvazan.test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi2.ColumnTypeEnum;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

public class TestColumnSlice {

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@Before
	public void createEntityManager() {
		Map<String, String> props = new HashMap<String, String>();
		props.put(NoSqlEntityManagerFactory.AUTO_CREATE_KEY, "create");
		factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY, props, null);
		mgr = factory.createEntityManager();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDbAndIndexesIfInMemoryType();
	}
	
	@Test
	public void testDecimalColumnSlice() throws UnsupportedEncodingException {
		NoSqlSession session = mgr.getSession();
		String colFamily = "float_indexes";
		
		DboDatabaseMeta meta = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		
		DboColumnMeta idMeta = new DboColumnMeta();
		idMeta.setup("id", null, String.class, ColumnTypeEnum.ID, null);
		
		DboTableMeta tableMeta = new DboTableMeta();
		tableMeta.setColumnFamily(colFamily);
		tableMeta.setColNameType(double.class);
		tableMeta.setRowKeyMeta(idMeta);
		
		mgr.put(idMeta);
		mgr.put(tableMeta);

		meta.addMetaClassDbo(tableMeta);
		mgr.put(meta);
		
		mgr.flush();
		
		byte[] rowKey = "myone_index".getBytes("UTF8");
		
		List<Column> columns = new ArrayList<Column>();
		
		columns.add(new Column(toDecBytes(5000.5), new byte[0]));
		columns.add(new Column(toDecBytes(20.333), new byte[0]));
		columns.add(new Column(toDecBytes(200.1111111111111), new byte[0]));
		columns.add(new Column(toDecBytes(10.9999999999999999999999999), new byte[0]));
		columns.add(new Column(toDecBytes(60.5), new byte[0]));
		columns.add(new Column(toDecBytes(700), new byte[0]));
		columns.add(new Column(toDecBytes(500023432430.44), new byte[0]));
		columns.add(new Column(toDecBytes(550.32), new byte[0]));
		columns.add(new Column(toDecBytes(340), new byte[0]));
		columns.add(new Column(toDecBytes(40.5), new byte[0]));
		columns.add(new Column(toDecBytes(-40.8888888888888888), new byte[0]));
		columns.add(new Column(toDecBytes(-200.23), new byte[0]));
		columns.add(new Column(toDecBytes(-500), new byte[0]));
		columns.add(new Column(toDecBytes(new BigDecimal("123000111222333444555666.66666666")), new byte[0]));
		columns.add(new Column(toDecBytes(new BigDecimal("-123000111222333444555666.888888")), new byte[0]));
		columns.add(new Column(toDecBytes(new BigDecimal("3")), new byte[0]));
		columns.add(new Column(toDecBytes(new BigDecimal("-3")), new byte[0]));
		
		session.persist(colFamily, rowKey, columns);
		session.flush();

		Iterable<Column> results = session.columnRangeScan(colFamily, rowKey, toDecBytes(-250), toDecBytes(12), 2);
		
		int counter = 0;
		for(Column col : results) {
			if(counter == 0) {
				byte[] data = col.getName();
				String hex = new String(Hex.encodeHex(data));
				Assert.assertEquals(-200.23, toDouble(col.getName()).doubleValue());
			}
			counter++;
		}
		Assert.assertEquals(5, counter);		
	}

	@Test
	public void testIntegerColumnSlice() throws UnsupportedEncodingException {
		NoSqlSession session = mgr.getSession();
		String colFamily = "time_indexes";
		
		DboDatabaseMeta meta = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		
		DboColumnMeta idMeta = new DboColumnMeta();
		idMeta.setup("id", null, String.class, ColumnTypeEnum.ID, null);
		
		DboTableMeta tableMeta = new DboTableMeta();
		tableMeta.setColumnFamily(colFamily);
		tableMeta.setColNameType(long.class);
		tableMeta.setRowKeyMeta(idMeta);
		
		mgr.put(idMeta);
		mgr.put(tableMeta);

		meta.addMetaClassDbo(tableMeta);
		mgr.put(meta);
		
		mgr.flush();
		
		byte[] rowKey = "myone_index".getBytes("UTF8");
		
		List<Column> columns = new ArrayList<Column>();
		
		columns.add(new Column(toIntBytes(500), new byte[0]));
		columns.add(new Column(toIntBytes(20), new byte[0]));
		columns.add(new Column(toIntBytes(200), new byte[0]));
		columns.add(new Column(toIntBytes(10), new byte[0]));
		columns.add(new Column(toIntBytes(60), new byte[0]));
		columns.add(new Column(toIntBytes(700), new byte[0]));
		columns.add(new Column(toIntBytes(500023432430L), new byte[0]));
		columns.add(new Column(toIntBytes(550), new byte[0]));
		columns.add(new Column(toIntBytes(340), new byte[0]));
		columns.add(new Column(toIntBytes(40), new byte[0]));
		columns.add(new Column(toIntBytes(-40), new byte[0]));
		columns.add(new Column(toIntBytes(-200), new byte[0]));
		columns.add(new Column(toIntBytes(-500), new byte[0]));
		columns.add(new Column(toIntBytes(new BigInteger("123000111222333444555666")), new byte[0]));
		columns.add(new Column(toIntBytes(new BigInteger("-123000111222333444555666")), new byte[0]));
		columns.add(new Column(toIntBytes(new BigInteger("3")), new byte[0]));
		columns.add(new Column(toIntBytes(new BigInteger("-3")), new byte[0]));
		
		session.persist(colFamily, rowKey, columns);
		session.flush();

		Iterable<Column> results = session.columnRangeScan(colFamily, rowKey, toIntBytes(-250), toIntBytes(50), 2);
		
		int counter = 0;
		for(Column col : results) {
			if(counter == 0)
				Assert.assertEquals(-200L, toLong(col.getName()).longValue());
			counter++;
		}
		Assert.assertEquals(7, counter);
	}
	
	private byte[] toIntBytes(Object obj) {
		return StandardConverters.convertToBytes(obj);
	}
	private byte[] toDecBytes(Object obj) {
		return StandardConverters.convertToDecimalBytes(obj);
	}

	private Long toLong(byte[] name) {
		return StandardConverters.convertFromBytes(Long.class, name);
	}
	private Double toDouble(byte[] name) {
		return StandardConverters.convertFromBytes(Double.class, name);
	}
}
