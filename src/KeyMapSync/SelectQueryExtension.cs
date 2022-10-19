using SqModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

internal static class SelectQueryExtension
{
    public static void RemoveSelectItem(this SelectQuery sq, List<string> whitelist)
    {
        sq.GetSelectItems().Where(x => !whitelist.Contains(x.Name)).ToList().ForEach(x =>
        {
            sq.Select.Collection.Remove(x);
        });
    }
}
