package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlPartitionKey;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;


@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findSecurity", query="select *  FROM TABLE e WHERE e.securityName = :security"),
	@NoSqlQuery(name="findAccount", query="select *  FROM TABLE e WHERE e.account = :account"),
	@NoSqlQuery(name="findByUnique", query="select * FROM TABLE e WHERE e.uniqueColumn = :unique")
})
public class PartitionTrade {

	@NoSqlId
	private String id;
	
	@NoSqlPartitionKey
	@NoSqlIndexed
	@NoSqlManyToOne
	private PartitionAccount account;

	@NoSqlPartitionKey
	@NoSqlIndexed
	private String securityName;
	
	@NoSqlIndexed
	private String uniqueColumn;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public PartitionAccount getAccount() {
		return account;
	}

	public void setAccount(PartitionAccount account) {
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


}
