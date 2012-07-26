package com.alvazan.test;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.Converters;
import com.alvazan.orm.api.spi.layer2.DboColumnMeta;
import com.alvazan.orm.api.spi.layer2.DboDatabaseMeta;
import com.alvazan.orm.api.spi.layer2.DboTableMeta;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;

public class TestColumnSlice {

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDbAndIndexesIfInMemoryType();
	}
	@Test
	public void testColumnSlice() throws UnsupportedEncodingException {
		NoSqlSession session = mgr.getSession();
		String colFamily = "time_indexes";
		
		DboDatabaseMeta meta = mgr.find(DboDatabaseMeta.class, NoSqlEntityManager.META_DB_KEY);
		
		DboColumnMeta idMeta = new DboColumnMeta();
		idMeta.setup("id", null, String.class, false);
		mgr.put(idMeta);
		
		DboTableMeta tableMeta = new DboTableMeta();
		tableMeta.setColumnFamily(colFamily);
		tableMeta.setColumnNameType(long.class);
		tableMeta.setRowKeyMeta(idMeta);
		mgr.put(tableMeta);

		meta.addMetaClassDbo(tableMeta);
		mgr.put(meta);
		
		mgr.flush();
		
		byte[] rowKey = "myone_index".getBytes("UTF8");
		
		List<Column> columns = new ArrayList<Column>();
		
		//columns.add(new Column(toBytes("asdfsd"), new byte[0]));
		//columns.add(new Column(toBytes("ewrwerewrewr"), new byte[0]));
		//columns.add(new Column(toBytes("eeeee"), new byte[0]));
		//columns.add(new Column(toBytes("0000000000"), new byte[0]));
		//columns.add(new Column(toBytes("a"), new byte[0]));
		
		columns.add(new Column(toBytes(500), new byte[0]));
		columns.add(new Column(toBytes(20), new byte[0]));
		columns.add(new Column(toBytes(200), new byte[0]));
		columns.add(new Column(toBytes(10), new byte[0]));
		columns.add(new Column(toBytes(60), new byte[0]));
		columns.add(new Column(toBytes(700), new byte[0]));
		columns.add(new Column(toBytes(500023432430L), new byte[0]));
		columns.add(new Column(toBytes(550), new byte[0]));
		columns.add(new Column(toBytes(340), new byte[0]));
		columns.add(new Column(toBytes(40), new byte[0]));
		columns.add(new Column(toBytes(-40), new byte[0]));
		columns.add(new Column(toBytes(-200), new byte[0]));
		columns.add(new Column(toBytes(-500), new byte[0]));
		columns.add(new Column(toBytes(new BigInteger("123000111222333444555666")), new byte[0]));
		columns.add(new Column(toBytes(new BigInteger("-123000111222333444555666")), new byte[0]));
		columns.add(new Column(toBytes(new BigInteger("3")), new byte[0]));
		columns.add(new Column(toBytes(new BigInteger("-3")), new byte[0]));
		
		long v = -300;
		
		byte[] bytes = toBytes(-300);
		String hex = new String(Hex.encodeHex(bytes));
		
		byte[] newBytes = new byte[8]; 
		newBytes[0] = (byte)(0xff & (v >> 56));
		newBytes[1] = (byte)(0xff & (v >> 48));
		newBytes[2] = (byte)(0xff & (v >> 40));
		newBytes[3] = (byte)(0xff & (v >> 32));
		newBytes[4] = (byte)(0xff & (v >> 24));
		newBytes[5] = (byte)(0xff & (v >> 16));
		newBytes[6] = (byte)(0xff & (v >>  8));
		newBytes[7] = (byte)(0xff & v);
		String hex2 = new String(Hex.encodeHex(newBytes));
		
		
		session.persist(colFamily, rowKey, columns );
		session.flush();

		Iterable<Column> results = session.columnRangeScan(colFamily, rowKey, toBytes(333), toBytes(555), 2);
		
		int counter = 0;
		for(Column col : results) {
			if(counter == 0)
				Assert.assertEquals(340L, toLong(col.getName()).longValue());
			counter++;
		}
		Assert.assertEquals(3, counter);
	}
	
	private byte[] toBytes(BigInteger bigInteger) {
		return Converters.BIGINTEGER_CONVERTER.convertToNoSql(bigInteger);
	}

	private byte[] toBytes(String string) {
		return Converters.STRING_CONVERTER.convertToNoSql(string);
	}

	private Long toLong(byte[] name) {
		return (Long)Converters.LONG_CONVERTER.convertFromNoSql(name);
	}

	private byte[] toBytes(long time) {
		return Converters.LONG_CONVERTER.convertToNoSql(time);
	}
}
