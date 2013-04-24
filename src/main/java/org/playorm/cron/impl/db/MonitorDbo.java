package org.playorm.cron.impl.db;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEmbedded;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

@NoSqlEntity
@NoSqlQuery(name="all", query="select m from TABLE as m")
public class MonitorDbo {

	@NoSqlId(usegenerator=false)
	private String id;
	
	@NoSqlIndexed
	private long timePeriodMillis;
	
	private String rawProperties = "";
	
	@NoSqlEmbedded
	private List<MonitorProperty> properties = new ArrayList<MonitorProperty>();

	/**
	 * This is the exact time it actually ran last time
	 */
	private DateTime lastRun;
	/**
	 * This is the exact time it should have run which is always off by +- the rate of runnable running
	 */
	private DateTime lastShouldHaveRun;

	private Long epochOffset;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTimePeriodMillis() {
		return timePeriodMillis;
	}

	public void setTimePeriodMillis(long timePeriodMillis) {
		this.timePeriodMillis = timePeriodMillis;
	}

	public DateTime getLastRun() {
		return lastRun;
	}

	public void setLastRun(DateTime lastRun) {
		this.lastRun = lastRun;
	}

	public DateTime getLastShouldHaveRun() {
		return lastShouldHaveRun;
	}

	public void setLastShouldHaveRun(DateTime lastShouldHaveRun) {
		this.lastShouldHaveRun = lastShouldHaveRun;
	}

	public static Cursor<KeyValue<MonitorDbo>> findAll(NoSqlEntityManager mgr) {
		Query<MonitorDbo> query = mgr.createNamedQuery(MonitorDbo.class, "all");
		return query.getResults();
	}

	public void setRawProperties(String props) {
		this.rawProperties = props;
	}

	public String getRawProperties() {
		return rawProperties;
	}

	public Long getEpochOffset() {
		return epochOffset;
	}

	public void setEpochOffset(Long epochOffset) {
		this.epochOffset = epochOffset;
	}
}
