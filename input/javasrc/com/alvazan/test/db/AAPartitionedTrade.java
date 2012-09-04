package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlColumn;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findJoinOnNullPartition", query="PARTITIONS p('account', :partId) select p FROM TABLE as p INNER JOIN p.security as s where s.securityType = :type and p.numShares = :shares"),
	@NoSqlQuery(name="findSecurity", query="PARTITIONS e('account', :acc) select *  FROM TABLE as e WHERE e.securityName = :security"),
	@NoSqlQuery(name="findAccount", query="PARTITIONS e('securityName', :secName) select *  FROM TABLE as e WHERE e.account = :account"),
	@NoSqlQuery(name="findByUnique", query="PARTITIONS e('account', :acc) select * FROM TABLE as e WHERE e.uniqueColumn = :unique")
})
public class AAPartitionedTrade {

	@NoSqlId
	private String id;
	
	@NoSqlPartitionByThisField
	@NoSqlIndexed
	@NoSqlManyToOne
	private PartAccount account;

	@NoSqlPartitionByThisField
	@NoSqlIndexed
	@NoSqlColumn(columnName="securityName")
	private String securityName;
	
	@NoSqlIndexed
	private String uniqueColumn;

	@NoSqlIndexed
	private int numShares;
	
	@NoSqlIndexed
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

	public static List<AAPartitionedTrade> findInPartition(NoSqlEntityManager mgr, int shares, String type, Account partitionId) {
		Query<AAPartitionedTrade> query = mgr.createNamedQuery(AAPartitionedTrade.class, "findJoinOnNullPartition");
		query.setParameter("shares", shares);
		query.setParameter("type", type);
		query.setParameter("partId", partitionId);
		return query.getResultList(0, null);
	}

	public static List<AAPartitionedTrade> findByUnique(NoSqlEntityManager mgr,
			String uniqueColumn2, PartAccount acc) {
		Query<AAPartitionedTrade> query = mgr.createNamedQuery(AAPartitionedTrade.class, "findByUnique");
		query.setParameter("unique", uniqueColumn2);
		query.setParameter("acc", acc);
		return query.getResultList(0, null);
	}

}
