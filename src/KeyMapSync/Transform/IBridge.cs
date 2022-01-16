using KeyMapSync.Entity;
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

    IPier GetCurrentPier();

    string Name { get; }

    (string commandText, IDictionary<string, object> parameter) ToInsertDestinationCommand(string prefix);

    (string commandText, IDictionary<string, object> parameter) ToReverseInsertDestinationCommand();

    (string commandText, IDictionary<string, object> parameter) ToInsertKeyMapCommand(string prefix);

    (string commandText, IDictionary<string, object> parameter) ToInsertOffsetKeyMapCommand();

    string ToRemoveKeyMapSql();

    (string commandText, IDictionary<string, object> parameter) ToInsertSyncCommand(string prefix);

    (string commandText, IDictionary<string, object> parameter) ToInsertVersionCommand();

    IList<string> ToExtensionSqls();
}

public interface IAbutment : IBridge
{
    Datasource Datasource { get; }

    /// <summary>
    /// ex."tmp01"
    /// </summary>
    string BridgeName { get; }

    string ToTemporaryViewDdl();
}

public interface IPier : IBridge
{

    IPier PreviousPrier { get; }

    string GetWithQuery();

    FilterContainer Filter { get; }

    string InnerAlias { get; }

    (string commandText, IDictionary<string, object> parameter) ToCreateTableCommand(bool isTemporary = true);

    IDictionary<string, object> ToCreateTableParameter();
}

