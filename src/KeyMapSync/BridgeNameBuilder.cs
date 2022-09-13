using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;

namespace KeyMapSync;

internal class BridgeNameBuilder
{
    public static string GetName(string datasource)
    {
        var bytes = Encoding.UTF8.GetBytes(datasource);
        using var alg = SHA512.Create();

        var sb = new StringBuilder();

        var hash = alg.ComputeHash(bytes);
        foreach (var item in hash) sb.Append(item.ToString("X2"));
        return sb.ToString();
    }
}
