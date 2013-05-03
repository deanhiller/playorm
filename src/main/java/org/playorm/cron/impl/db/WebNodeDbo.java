package org.playorm.cron.impl.db;

import org.joda.time.DateTime;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

@NoSqlEntity
@NoSqlQuery(name="all", query="select s from TABLE as s")
public class WebNodeDbo {

	@NoSqlId(usegenerator=false)
	private String webServerName;
	
	private DateTime lastSeen;

	//purely for index and NOT on isUp on purpose as we could get corrupt index too easily with computers running
	@NoSqlIndexed
	private String indexed = "webnode";
	
	private boolean isUp;
	
	@Override
	public String toString() {
		return "[webNode='"+webServerName+"' up="+isUp+" lastSeen="+lastSeen+"]";
	}

	public String getWebServerName() {
		return webServerName;
	}

	public void setWebServerName(String webServerName) {
		this.webServerName = webServerName;
	}

	public DateTime getLastSeen() {
		return lastSeen;
	}

	public void setLastSeen(DateTime lastSeen) {
		this.lastSeen = lastSeen;
	}
	
	public boolean isUp() {
		return isUp;
	}

	public void setUp(boolean isUp) {
		this.isUp = isUp;
	}

	public static Cursor<KeyValue<WebNodeDbo>> findAllNodes(NoSqlEntityManager mgr) {
		Query<WebNodeDbo> query = mgr.createNamedQuery(WebNodeDbo.class, "all");
		return query.getResults();
	}
}
