using System;

namespace KeyMapSync
{
    public class SyncEventArgs : EventArgs
    {
        public SyncEventArgs(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }
}