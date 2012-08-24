package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlColumn;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findByNumber", query="PARTITIONS e(:partitionId) select * FROM TABLE as e WHERE e.number = :number")
})
public class PartitionedSingleTrade {

	@NoSqlId
	private String id;

	@NoSqlPartitionByThisField
	@NoSqlIndexed
	@NoSqlColumn(columnName="securityName")
	private String securityName;
	
	@NoSqlIndexed
	private int number;

	private int unique;
	
	public int getUnique() {
		return unique;
	}

	public void setUnique(int unique) {
		this.unique = unique;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSecurityName() {
		return securityName;
	}

	public void setSecurityName(String securityName) {
		this.securityName = securityName;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
}
