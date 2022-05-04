using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Data;

public interface IDatasource
{
    string DestinaionTableName { get; }

    string MappingName { get; }

    IEnumerable<string> DatasourceKeyColumns { get; }

    string WithQueryText { get; }

    string AliasName { get; }

    ParameterSet ParameterSet { get; }

    IList<ExtensionDatasource> Extensions { get; }
}

