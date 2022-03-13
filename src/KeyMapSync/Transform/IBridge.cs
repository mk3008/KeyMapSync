﻿using KeyMapSync.Entity;
using KeyMapSync.Filtering;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Transform;

public interface IBridge
{
    IAbutment GetAbutment();

    IPier? GetCurrentPier();

    string ViewOrCteName { get; }
}

public interface IAbutment : IBridge
{
    Datasource Datasource { get; }

    public string ViewName { get; }
}

public interface IPier : IBridge
{

    IPier? PreviousPrier { get; }

    string ToSelectQuery();

    FilterContainer Filter { get; }

    public string CteName { get; }
}

