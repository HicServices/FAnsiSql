﻿using System;

namespace FAnsi.Discovery.QuerySyntax.Aggregation
{
    /// <summary>
    /// Thrown when a <see cref="AggregateCustomLineCollection"/> is created in an illegal state (e.g. an axis is defined but no corresponding
    /// SELECT / GROUP by <see cref="CustomLine"/> are provided.
    /// </summary>
    public class AggregateCustomLineCollectionException : Exception
    {
        public AggregateCustomLineCollectionException(string msg):base(msg)
        {
            
        }

    }
}