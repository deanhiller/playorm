/**
 * @COPYRIGHT (C) 2010 Hotel Reservation Service (HRS) Robert Ragge GmbH
 *
 * All rights reserved
 */
package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Robert Stupp (last modified by $Author$)
 * @version $Revision$ $Date$
 */
@NoSqlDiscriminatorColumn("nestedB")
public class InheritedNestedB extends InheritedNestedBaseForB {
    @NoSqlOneToMany(columnName = "cs")
    private Set<InheritedNestedBaseForC> cs = new HashSet<InheritedNestedBaseForC>();

    public Set<InheritedNestedBaseForC> getCs() {
        return cs;
    }
}
