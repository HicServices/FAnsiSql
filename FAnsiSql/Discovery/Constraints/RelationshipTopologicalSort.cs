﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using FAnsi.Exceptions;

namespace FAnsi.Discovery.Constraints;

/// <summary>
/// Helps resolve a dependency order between a collection of tables with interlinking foreign key constraints.  Implements Khan's algorithm.
/// </summary>
public sealed class RelationshipTopologicalSort
{
    /// <summary>
    /// The dependency order from least dependent (isolated tables and parent tables) to most (child tables then grandchild tables).
    /// </summary>
    public IReadOnlyList<DiscoveredTable> Order => new ReadOnlyCollection<DiscoveredTable>(_sortedList);

    private readonly List<DiscoveredTable> _sortedList;

    /// <summary>
    /// <para>Connects to the database and discovers relationships between <paramref name="tables"/> then generates a sort order of dependency in which
    /// all primary key tables should appear before thier respective foreign key tables.</para>
    /// 
    /// <para>Calling this method will result in database queries being executed (to discover keys)</para>
    /// 
    ///  <para>Sort order is based on Khan's algorithm (https://en.wikipedia.org/wiki/Topological_sorting)</para>
    /// </summary>
    /// <param name="tables"></param>
    /// <exception cref="Exceptions.CircularDependencyException"></exception>
    public RelationshipTopologicalSort(IEnumerable<DiscoveredTable> tables)
    {
        var nodes = new HashSet<DiscoveredTable>(tables);
        var edges = new HashSet<Tuple<DiscoveredTable, DiscoveredTable>>();

        foreach (var relationship in nodes
                     .Select(table => table.DiscoverRelationships().Where(r => nodes.Contains(r.ForeignKeyTable)))
                     .SelectMany(static relevantRelationships => relevantRelationships))
            edges.Add(Tuple.Create(relationship.PrimaryKeyTable, relationship.ForeignKeyTable));

        _sortedList = TopologicalSort(nodes, edges);
    }

    /// <summary>
    /// Topological Sorting (Kahn's algorithm)
    /// </summary>
    /// <remarks>https://en.wikipedia.org/wiki/Topological_sorting</remarks>
    /// <typeparam name="T"></typeparam>
    /// <param name="nodes">All nodes of directed acyclic graph.</param>
    /// <param name="edges">All edges of directed acyclic graph.</param>
    /// <returns>Sorted node in topological order.</returns>
    private static List<T> TopologicalSort<T>(IEnumerable<T> nodes, HashSet<Tuple<T, T>> edges) where T : IEquatable<T>
    {
        // Empty list that will contain the sorted elements
        var l = new List<T>();

        // Set of all nodes with no incoming edges
        var s = new HashSet<T>(nodes.Where(n => edges.All(e => !e.Item2.Equals(n))));

        // while S is non-empty do
        while (s.Count != 0)
        {

            //  remove a node n from S
            var n = s.First();
            s.Remove(n);

            // add n to tail of L
            l.Add(n);

            // for each node m with an edge e from n to m do
            foreach (var e in edges.Where(e => e.Item1.Equals(n)).ToList())
            {
                var m = e.Item2;

                // remove edge e from the graph
                edges.Remove(e);

                // if m has no other incoming edges then
                if (edges.All(me => !me.Item2.Equals(m)))
                    // insert m into S
                    s.Add(m);
            }
        }

        // if graph has edges then
        if (edges.Count != 0)
            // return error (graph has at least one cycle)
            throw new CircularDependencyException(FAnsiStrings.RelationshipTopologicalSort_FoundCircularDependencies);


        // return L (a topologically sorted order)
        return l;
    }
}