namespace KeyMapSync
{
    public class TableNameInfo
    {
        public string SchemaName { get; set; } = "";

        public string TableName { get; set; } = "";

        public string TableFullName
        {
            get
            {
                if (string.IsNullOrEmpty(SchemaName)) return TableName;
                return $"{SchemaName}.{TableName}";
            }
        }
    }
}