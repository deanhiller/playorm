package com.alvazan.orm.api.base.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;
import com.alvazan.orm.api.spi2.DboColumnCommonMeta;
import com.alvazan.orm.api.spi2.DboColumnIdMeta;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;


public class PerformanceWriteTest {

	private static final Logger log = LoggerFactory.getLogger(PerformanceWriteTest.class);
	private static final int NUM_THREADS = 30;
	
	private ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
	private Timer timer = new Timer();
	private boolean shouldRun = true;
	private long totalRowsWritten = 0;
	private int logEveryN;
	private int numThreadsLeft = NUM_THREADS;
	
	public static void main(String[] args) {
//		if(args.length != 1) {
//			log.warn("Run java com.alvazan.orm.api.base.util.PerformanceWriteTest <timeInSeconds>");
//			return;
//		}
//		
//		int timeInSeconds = Integer.parseInt(args[0]);
		
		int timeInSeconds = 10*60;
		int numColumns = 50;
		int logEveryNRows = 1000;
		new PerformanceWriteTest().start(timeInSeconds, numColumns, logEveryNRows);
	}

	private synchronized void finished() {
		numThreadsLeft--;
		if(numThreadsLeft == 0)
			log.info("final count of rows="+totalRowsWritten);
	}
	
	private synchronized void addCount(long count) {
		totalRowsWritten += count;
		if(totalRowsWritten % logEveryN == 0) 
			log.info("total rows written so far="+totalRowsWritten);
	}
	
	private void start(int timeInMinutes, int numColumns, int logEveryNRows) {
		logEveryN = logEveryNRows;
		//BEFORE Timer, let's get setup first
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		NoSqlEntityManagerFactory factory = Bootstrap.create(DbTypeEnum.CASSANDRA, props, null, null);
		DboTableMeta table = setupMetaData(numColumns, factory);
		
		timer.schedule(new StopTask(), timeInMinutes*1000);
		
		for(int i = 0; i < NUM_THREADS; i++) {
			Runnable r = new SlamDataIn(factory, table);
			exec.execute(r);
		}
		
		exec.shutdown();
	}

	private DboTableMeta setupMetaData(int numColumns, NoSqlEntityManagerFactory factory) {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		DboDatabaseMeta meta = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		if(meta != null) {
			meta = new DboDatabaseMeta();
		}
		
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup("id", String.class, null);
		
		DboTableMeta table = new DboTableMeta();
		table.setColumnFamily("testWrites");
		table.setRowKeyMeta(idMeta);
		
		for(int i = 0; i < numColumns; i++) {
			DboColumnCommonMeta col = new DboColumnCommonMeta();
			col.setup("col"+i, long.class, null);
			
			table.addColumnMeta(col);
			mgr.put(col);
		}
		
		meta.addMetaClassDbo(table);
		
		mgr.put(idMeta);
		mgr.put(table);
		mgr.put(meta);
		mgr.flush();
		
		return table;
	}
	
	private class SlamDataIn implements Runnable {
		private NoSqlEntityManagerFactory mgrFactory;
		private DboTableMeta table;
		private Random r = new Random(System.currentTimeMillis());
		
		public SlamDataIn(NoSqlEntityManagerFactory mgrFactory, DboTableMeta table) {
			this.mgrFactory = mgrFactory;
			this.table = table;
		}

		@Override
		public void run() {
			long count = 0;
			
			NoSqlEntityManager mgr = mgrFactory.createEntityManager();
			NoSqlSession session = mgr.getSession();
			String colFamily = table.getColumnFamily();
			
			while(shouldRun) {
				
				//let's write 20 rows for every flush we do..
				for(int i = 0; i < 20; i++) {
					String rowKey = UniqueKeyGenerator.generateKey();
					byte[] key = table.getIdColumnMeta().convertToStorage2(rowKey);
					List<Column> columns = createColumns(i, table);
					session.put(colFamily, key, columns);
					count++;
				}
				addCount(count);
				count = 0;
				
				session.flush();
			}
			
			finished();
		}

		private List<Column> createColumns(int i, DboTableMeta table2) {
			List<Column> cols = new ArrayList<Column>();
			for(DboColumnMeta colMeta : table2.getAllColumns()) {
				Column c = new Column();
				byte[] name = colMeta.getColumnNameAsBytes();
				c.setName(name);
				long val = r.nextLong();
				byte[] value = colMeta.convertToStorage2(val);
				c.setValue(value);
				cols.add(c);
			}
			return cols;
		}
	}
	
	private class StopTask extends TimerTask {
		@Override
		public void run() {
			shouldRun = false;
		}
	}
}
