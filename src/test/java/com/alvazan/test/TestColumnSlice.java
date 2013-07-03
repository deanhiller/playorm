package com.alvazan.test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.TimeValColumn;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

public class TestColumnSlice {

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@Before
	public void createEntityManager() {
		factory = FactorySingleton.createFactoryOnce();
		mgr = factory.createEntityManager();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}

	@Test
	public void testTimeSeriesData() {
		NoSqlTypedSession typedSession = mgr.getTypedSession();
		String colFamily = "timeSeriesAutoPartition";

		long partitionSize = TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);
		DboTableMeta tm = new DboTableMeta();
		tm.setTimeSeries(true);
		tm.setTimeSeriesPartionSize(partitionSize);
		tm.setup(colFamily, "realCf", false);
		tm.setColNameType(long.class);

		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(tm, "time", BigInteger.class, false);
		
		DboColumnCommonMeta meta = new DboColumnCommonMeta();
		meta.setup(tm, "value", BigDecimal.class, false, false);

		mgr.put(idMeta);
		mgr.put(meta);
		mgr.put(tm);

		mgr.flush();
		
		TypedRow row = typedSession.createTypedRow(colFamily);
		BigInteger rowKey = new BigInteger("0");
		row.setRowKey(rowKey);
		row.addTimeValue(0, new BigDecimal(56));
		row.addTimeValue(10, new BigDecimal(56));
		row.addTimeValue(20, new BigDecimal(56));
		row.addTimeValue(30, new BigDecimal(56));
		row.addTimeValue(40, new BigDecimal(56));
		row.addTimeValue(50, new BigDecimal(56));
		row.addTimeValue(60, new BigDecimal(56));
		row.addTimeValue(70, new BigDecimal(56));
		row.addTimeValue(80, new BigDecimal(56));
		row.addTimeValue(90, new BigDecimal(56));
		row.addTimeValue(100, new BigDecimal(56));

		typedSession.put(colFamily, row);
		typedSession.flush();
		
		Cursor<TimeValColumn> timeVal = typedSession.timeSeriesSlice(colFamily, rowKey, 40, 60, 200);
		for(int i = 0; i < 3; i++) {
			timeVal.next();
			TimeValColumn current = timeVal.getCurrent();
			Assert.assertEquals(i*10+40, current.getTime());
		}
	}

	@Test
	public void testDecimalColumnSlice() throws UnsupportedEncodingException {
		NoSqlSession session = mgr.getSession();
		String colFamily = "float_indexes";
		
		DboDatabaseMeta meta = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);

		DboTableMeta tableMeta = new DboTableMeta();
		tableMeta.setup(null, colFamily, false);
		tableMeta.setColNameType(double.class);
		
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(tableMeta, "id", String.class, false);
		
		mgr.put(idMeta);
		mgr.put(tableMeta);

		meta.addMetaClassDbo(tableMeta);
		mgr.put(meta);
		
		mgr.flush();
		
		byte[] rowKey = StandardConverters.convertToBytes("myone_index");
		
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
		
		session.put(tableMeta, rowKey, columns);
		session.flush();

		byte[] from = toDecBytes(-250);
		byte[] to = toDecBytes(12);
		Cursor<Column> results = session.columnSlice(tableMeta, rowKey, from, to, 2);//(scanInfo, from, to, 2);
		
		int counter = 0;
		while(results.next()) {
			Column col = results.getCurrent();
			if(counter == 0) {
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

		DboTableMeta tableMeta = new DboTableMeta();
		tableMeta.setup(null, colFamily, false);
		tableMeta.setColNameType(long.class);
		
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(tableMeta, "id", String.class, false);
		
		mgr.put(idMeta);
		mgr.put(tableMeta);

		meta.addMetaClassDbo(tableMeta);
		mgr.put(meta);
		
		mgr.flush();
		
		byte[] rowKey = StandardConverters.convertToBytes("myone_index");
		
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
		
		session.put(tableMeta, rowKey, columns);
		session.flush();

		byte[] from = toIntBytes(-250);
		byte[] to = toIntBytes(50);
		Cursor<Column> results = session.columnSlice(tableMeta, rowKey, from, to, 2);
		
		int counter = 0;
		while(results.next()) {
			Column col = results.getCurrent();
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
