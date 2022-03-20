using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace KeyMapSync.Transform;

public static class IBridgeExtension
{
    //public static string GetBridgeName(this IBridge source)
    //{
    //    return source.GetAbutment().BridgeName;
    //}

    public static Datasource GetDatasource(this IBridge source)
    {
        return source.Abutment.Datasource;
    }

    public static Destination GetDestination(this IBridge source)
    {
        return source.Abutment.Datasource.Destination;
    }
}