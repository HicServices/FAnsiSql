﻿using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Connections;

namespace FAnsi.Discovery.Constraints
{
    /// <summary>
    /// A foreign key relationship between two database tables.
    /// </summary>
    public class DiscoveredRelationship
    {
        /// <summary>
        /// The name of the foreign key constraint in the database e.g. FK_Table1_Table2
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// The table in which the primary key is declared.  This is the parent table.
        /// </summary>
        public DiscoveredTable PrimaryKeyTable { get; private set; }

        /// <summary>
        /// The table which contains child records.
        /// </summary>
        public DiscoveredTable ForeignKeyTable { get; private set; }

        /// <summary>
        /// Mapping of primary key column(s) in <see cref="PrimaryKeyTable"/> to foreign key column(s) in <see cref="ForeignKeyTable"/>.  If there are more than one entry 
        /// then the foreign key is a composite key.
        /// </summary>
        public Dictionary<DiscoveredColumn, DiscoveredColumn> Keys { get; private set; }

        /// <summary>
        /// Describes what happens to records in the <see cref="ForeignKeyTable"/> when thier parent records (in the <see cref="PrimaryKeyTable"/>) are deleted.
        /// </summary>
        public CascadeRule CascadeDelete { get; private set; }

        private DiscoveredColumn[] _pkColumns;
        private DiscoveredColumn[] _fkColumns;

        /// <summary>
        /// Internal API constructor intended for Implementation classes, instead use <see cref="DiscoveredTable.DiscoverRelationships"/> instead.
        /// </summary>
        /// <param name="fkName"></param>
        /// <param name="pkTable"></param>
        /// <param name="fkTable"></param>
        /// <param name="deleteRule"></param>
        public DiscoveredRelationship(string fkName, DiscoveredTable pkTable, DiscoveredTable fkTable, CascadeRule deleteRule)
        {
            Name = fkName;
            PrimaryKeyTable = pkTable;
            ForeignKeyTable = fkTable;
            CascadeDelete = deleteRule;
            
            Keys = new Dictionary<DiscoveredColumn, DiscoveredColumn>();
        }

        /// <summary>
        /// Discovers and adds the provided pair to <see cref="Keys"/>.  Column names must be members of <see cref="PrimaryKeyTable"/> and <see cref="ForeignKeyTable"/> (respectively)
        /// </summary>
        /// <param name="primaryKeyCol"></param>
        /// <param name="foreignKeyCol"></param>
        /// <param name="transaction"></param>
        public void AddKeys(string primaryKeyCol, string foreignKeyCol,IManagedTransaction transaction = null)
        {
            if (_pkColumns == null)
            {
                _pkColumns = PrimaryKeyTable.DiscoverColumns(transaction);
                _fkColumns = ForeignKeyTable.DiscoverColumns(transaction);
            }

            Keys.Add(
                    _pkColumns.Single(c=>c.GetRuntimeName().Equals(primaryKeyCol,StringComparison.CurrentCultureIgnoreCase)),
                    _fkColumns.Single(c => c.GetRuntimeName().Equals(foreignKeyCol, StringComparison.CurrentCultureIgnoreCase))
                );

        }
    }
}