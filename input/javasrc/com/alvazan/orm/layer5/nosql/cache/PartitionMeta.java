package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.layer5.indexing.ExpressionNode;

public class PartitionMeta {

	private DboColumnMeta partitionColumn;
	private ExpressionNode node;

	public PartitionMeta(DboColumnMeta partitionColumn, ExpressionNode node) {
		this.partitionColumn = partitionColumn;
		this.node = node;
	}

	public DboColumnMeta getPartitionColumn() {
		return partitionColumn;
	}

	public ExpressionNode getNode() {
		return node;
	}

}
