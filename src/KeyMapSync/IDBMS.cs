using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync;

public interface IDBMS {

    string ToKeyMapDDL(Datasource ds);

    string ToSyncDDL(Datasource ds);

    string ToVersionDDL(Datasource ds);

    string ToOffsetDDL(Datasource ds);
}

