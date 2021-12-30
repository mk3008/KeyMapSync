﻿using KeyMapSync.Filtering;
using KeyMapSync.Transform;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Filtering;

public class CustomFilter : IFilter
{
    public string Condition { get; set; }

    public ExpandoObject Parameter { get; set; }

    public string ToCondition(IBridge sender)
    {
        return Condition;
    }

    public ExpandoObject ToParameter()
    {
        return Parameter;
    }
}