package com.alvazan.orm.layer9z.spi.db.cassandracql3;


import java.nio.ByteBuffer;

import java.util.List;

import com.alvazan.orm.api.z8spi.Key;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;


public class Cql3Util {
	public static IndexColumn convertToIndexCol(com.datastax.driver.core.Row row, String tableName) {
	    Object indValue = null;
        if (tableName.equalsIgnoreCase("StringIndice")) {
            indValue = row.getString("colname");
        } else if (tableName.equalsIgnoreCase("IntegerIndice")) {
            indValue = row.getLong("colname");
        } else if (tableName.equalsIgnoreCase("DecimalIndice")) {
            indValue = row.getFloat("colname");
        }
        ByteBuffer data = row.getBytes("colvalue");
        byte[] val = new byte[data.remaining()];
        data.get(val);
		IndexColumn c = new IndexColumn();
		// c.setColumnName(columnName); Will we ever need this now?
		if (val != null) {
			c.setPrimaryKey(val);
		}
		if (indValue != null) {
			c.setIndexedValue(StandardConverters.convertToBytes(indValue));
		}

		c.setValue(null);
		return c;
	}

	public static Where createRowQuery(Key from, Key to, DboColumnMeta colMeta, Select selectQuery, String rowKey) {
        Where selectWhere = selectQuery.where();
        Clause rkClause = QueryBuilder.eq("id", rowKey);
        selectWhere.and(rkClause);

        Object valFrom = null, valTo = null;
		if (colMeta != null) {
			if (from != null) {
				valFrom = colMeta.getStorageType().convertFromNoSql(from.getKey());
                valFrom = checkForBoolean(valFrom);
			}
			if (to != null) {
				valTo = colMeta.getStorageType().convertFromNoSql(to.getKey());
                valTo = checkForBoolean(valTo);
			}
		} else
			return selectWhere;

        if (from != null) {
            if (from.isInclusive()) {
                Clause gteClause = QueryBuilder.gte("colname", valFrom);
                selectWhere.and(gteClause);
            } else {
                Clause gtClause = QueryBuilder.gt("colname", valFrom);
                selectWhere.and(gtClause);
            }

        }
        if (to != null) {
            if (to.isInclusive()) {
                Clause lteClause = QueryBuilder.lte("colname", valTo);
                selectWhere.and(lteClause);
            }
            else {
                Clause ltClause = QueryBuilder.lt("colname", valTo);
                selectWhere.and(ltClause);
            }
		}
		return selectWhere;
	}

    public static Where createRowQueryFromValues(List<byte[]> values, DboColumnMeta colMeta, Select selectQuery, String rowKey) {
        Where selectWhere = selectQuery.where();

        Clause rkClause = QueryBuilder.eq("id", rowKey);
        selectWhere.and(rkClause);

        Object[] valStrings = new Object[values.size()];
        int count = 0;
        for (byte[] value : values) {
            valStrings[count] = StandardConverters.convertFromBytes(String.class, value);
            count++;
        }
        
        Clause inClause = QueryBuilder.in("colname", valStrings);
        selectWhere.and(inClause);
        return selectWhere;
    }

    public static Object checkForBoolean(Object val) {
        if (val == null)
            return null;
        else if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            if (b)
                return 1;
            else
                return 0;
        }
        return val;
    }

}
