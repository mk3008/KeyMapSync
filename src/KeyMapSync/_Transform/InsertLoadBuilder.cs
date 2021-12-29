using Dapper;
using KeyMapSync.Data;
using KeyMapSync.Load;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace KeyMapSync.Transform
{
    public partial class InsertLoadBuilder
    {
        protected DbManager Manager { get; set; }

        private const string BRIDGE_ALIAS = "_bridge";

        private const string SYNC_ALIAS = "_sync";

        private const string KEYMAP_ALIAS = "_keymap";

        public ILoad Build(IDatasource datasource, ParameterSet bridgeFiler = null)
        {

            // Datasouce
            //   -> Bridge
            //     -> Destination
            //        -> Version
            //        -> Sync
            //        -> KeyMap
            //        -> Extension1
            //        -> ..
            //        -> ExtensionN

            var destTable = Manager.ReadDestinationTable(datasource);
            var versionTable = Manager.ReadOrCreateVersionTable(datasource);
            var syncTable = Manager.ReadOrCreateSyncTable(datasource);
            var keymapTable = Manager.ReadOrCreateKeyMapTable(datasource);

            UnSynchronizedCondition unsyncfilter = null;
            if (string.IsNullOrEmpty(datasource.MappingName))
            {
                unsyncfilter = new UnSynchronizedCondition()
                {
                    Datasource = datasource,
                    KeyMapTableName = keymapTable.TableName,
                    KeyMapAliasName = KEYMAP_ALIAS,
                };
            }

            var bridge = new BridgeLoad()
            {
                Manager = Manager,
                Datasource = datasource,
                BridgeTableName = Manager.GetNextBridgeName(),
                DestinationSequence = destTable.SequenceColumn,
                UnSynchronizedFilter = unsyncfilter,
            };

            var dest = new DestinationLoad()
            {
                Manager = Manager,
                BridgeTableName = bridge.BridgeTableName,
                BridgeAliasName = BRIDGE_ALIAS,
                DestinationTableName = destTable.TableFullName,
                BridgeFilter = bridgeFiler,
            };
            bridge.Loads.Add(dest);

            if (unsyncfilter != null)
            {
                var version = new VersionLoad()
                {
                    Manager = Manager,
                    VersionColumnName = versionTable.SequenceColumn,
                    DestinationTableName = versionTable.TableFullName,
                };
                dest.Cascades.Add(version);

                var sync = new SyncLoad()
                {
                    Manager = Manager,
                    BridgeTableName = bridge.BridgeTableName,
                    SyncTableName = syncTable.TableFullName,
                    VersionLoad = version,
                    BridgeFilter = bridgeFiler,
                };
                dest.Cascades.Add(sync);

                var keyamp = new KeyMapLoad()
                {
                    Manager = Manager,
                    BridgeTableName = bridge.BridgeTableName,
                    KeyMapTableName = keymapTable.TableFullName,
                    Datasource = datasource,
                    BridgeFilter = bridgeFiler,
                };
                dest.Cascades.Add(sync);
            }

            foreach (var item in datasource.Extensions)
            {
                dest.Cascades.Add(Build(item, bridgeFiler));
            }

            return bridge;
        }

        public ILoad Build(VersionValidateFilter container)
        {
            // Datasouce
            // -> ExpectBridge
            //    -> RemoveBridge
            //       -> Destination
            //          -> Version
            //          -> Sync
            //          -> OffsetKeymap
            //       -> KeyMap(delete)

            var datasource = container.Datasource;

            var destTable = Manager.ReadDestinationTable(datasource);
            var versionTable = Manager.ReadOrCreateVersionTable(datasource);
            var syncTable = Manager.ReadOrCreateSyncTable(datasource);
            var keymapTable = Manager.ReadOrCreateKeyMapTable(datasource);
            var offsetTable = Manager.ReadOrCreateOffsetMapTable(datasource);

            var expectBridge = new ExpectBridgeLoad()
            {
                Manager = Manager,
                Datasource = datasource,
                ExpectBridgeTableName = Manager.GetNextBridgeName(),
                DestinationKeyColumnName = destTable.SequenceColumn.ColumnName,
                KeymapTableName = keymapTable.TableName,
                KeymapTableAliasName = KEYMAP_ALIAS,
                OffsetDataroucePrefix = "", 
                SyncTableName = syncTable.TableName,
                SyncTableAliasName = SYNC_ALIAS, 
                ValidateFilter = container,
                VersionColumnName = versionTable.SequenceColumn.ColumnName
            };


            var removeBridge = new RemoveBridgeLoad()
            {
                Manager = Manager,
                Destination = Manager.GetNextBridgeName(),
                BridgeTableName = Manager.GetNextBridgeName(),
                DestinationSequence = destTable.SequenceColumn,
                SynchronizedFilter = syncfiler,
                ExpectFilter = container.ToRemoveParameterSet(destTable, )

            };


            return expectBridge;
        }
    }
}
