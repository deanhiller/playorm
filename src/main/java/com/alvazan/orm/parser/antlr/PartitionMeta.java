package com.alvazan.orm.parser.antlr;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class PartitionMeta {

	private DboColumnMeta partitionColumn;
	private ParsedNode node;

	public PartitionMeta(DboColumnMeta partitionColumn, ParsedNode node) {
		this.partitionColumn = partitionColumn;
		this.node = node;
	}

	public DboColumnMeta getPartitionColumn() {
		return partitionColumn;
	}

	public ParsedNode getNode() {
		return node;
	}

}
