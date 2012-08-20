package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;


@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findSecurity", query="select *  FROM TABLE e WHERE e.securityName = :security"),
	@NoSqlQuery(name="findAccount", query="select *  FROM TABLE e WHERE e.account = :account"),
	@NoSqlQuery(name="findByUnique", query="select * FROM TABLE e WHERE e.uniqueColumn = :unique")
})
public class PartitionedTrade {

	@NoSqlId
	private String id;
	
	@NoSqlPartitionByThisField
	@NoSqlIndexed
	@NoSqlManyToOne
	private PartAccount account;

	@NoSqlPartitionByThisField
	@NoSqlIndexed
	private String securityName;
	
	@NoSqlIndexed
	private String uniqueColumn;

	@NoSqlIndexed
	private int numShares;
	
	@NoSqlManyToOne
	private PartSecurity security;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public PartAccount getAccount() {
		return account;
	}

	public void setAccount(PartAccount account) {
		this.account = account;
	}

	public String getSecurityName() {
		return securityName;
	}

	public void setSecurityName(String securityName) {
		this.securityName = securityName;
	}

	public String getUniqueColumn() {
		return uniqueColumn;
	}

	public void setUniqueColumn(String uniqueColumn) {
		this.uniqueColumn = uniqueColumn;
	}

	public PartSecurity getSecurity() {
		return security;
	}

	public void setSecurity(PartSecurity security) {
		this.security = security;
	}

	public int getNumShares() {
		return numShares;
	}

	public void setNumShares(int numShares) {
		this.numShares = numShares;
	}

	public static List<PartitionedTrade> findInNullPartition(NoSqlEntityManager mgr, int start, int end) {
		Partition<PartitionedTrade> partition = mgr.getPartition(PartitionedTrade.class, "account", null);
		Query<PartitionedTrade> query = partition.createNamedQuery("findInNullPartition");
		query.setParameter("start", start);
		query.setParameter("end", end);
		return query.getResultList(0, null);
	}

}
