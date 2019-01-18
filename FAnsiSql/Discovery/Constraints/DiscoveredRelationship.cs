using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Connections;

namespace FAnsi.Discovery.Constraints
{
    public class DiscoveredRelationship
    {
        public string Name { get; private set; }

        public DiscoveredTable PrimaryKeyTable { get; private set; }
        public DiscoveredTable ForeignKeyTable { get; private set; }

        /// <summary>
        /// Mapping of primary key columns in <see cref="PrimaryKeyTable"/> to foreign key columns in <see cref="ForeignKeyTable"/>
        /// </summary>
        public Dictionary<DiscoveredColumn, DiscoveredColumn> Keys { get; private set; }

        public CascadeRule CascadeDelete { get; private set; }

        private DiscoveredColumn[] _pkColumns;
        private DiscoveredColumn[] _fkColumns;

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


    public class TableDependencyOrder
    {
        Dictionary<DiscoveredTable,DiscoveredTable[]>  tableDependencies = new Dictionary<DiscoveredTable, DiscoveredTable[]>();

        public TableDependencyOrder(DiscoveredTable[] tables)
        {
            foreach (DiscoveredTable table in tables)
            {
                //find all foreign key relationships to other tables in the set
                table.DiscoveredRelationships().Where(t => tables.Contains(t.ForeignKeyTable));
            }
        }

        public int GetOrderFor(DiscoveredTable table)
        {
            throw new NotImplementedException();
        }
    }
}