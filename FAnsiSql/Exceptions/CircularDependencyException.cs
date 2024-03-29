﻿using System;

namespace FAnsi.Exceptions;

/// <summary>
/// Thrown when the foreign key constraints between a set of tables result in a circular reference (A depends on B, B depends on C, C depends on A)
/// </summary>
public sealed class CircularDependencyException(string msg) : Exception(msg);