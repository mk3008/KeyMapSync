namespace KeyMapSync
{
    /// <summary>
    /// sqlquence column
    /// </summary>
    public class SequenceColumn
    {
        /// <summary>
        /// column name
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// next value sequence command text.
        /// </summary>
        public string NextValCommand { get; set; }
    }
}