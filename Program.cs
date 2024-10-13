using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace DatabaseTableTransfer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter the source connection string:");
            string sourceConnectionString = Console.ReadLine();

            Console.WriteLine("Enter the destination connection string:");
            string destinationConnectionString = Console.ReadLine();

            Console.WriteLine("Enter the table name to transfer:");
            string sourceTableName = Console.ReadLine();

            Console.WriteLine("Enter destination table name (leave blank to match source table name):");
            string destinationTableName = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(destinationTableName))
            {
                destinationTableName = sourceTableName;
            }

            try
            {
                TransferTable(sourceConnectionString, destinationConnectionString, sourceTableName, destinationTableName);
                Console.WriteLine("Table transferred successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        static void TransferTable(string sourceConnectionString, string destinationConnectionString, string sourceTableName, string destinationTableName)
        {
            using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();

                // Get table schema
                string getSchemaQuery = $"SELECT * FROM {sourceTableName} WHERE 1 = 0";
                SqlCommand schemaCommand = new SqlCommand(getSchemaQuery, sourceConnection);
                SqlDataAdapter adapter = new SqlDataAdapter(schemaCommand);
                DataTable schemaTable = new DataTable();
                adapter.Fill(schemaTable);

                // Generate CREATE TABLE script
                StringBuilder createTableQuery = new StringBuilder();
                createTableQuery.Append($"CREATE TABLE {destinationTableName} (");
                foreach (DataColumn column in schemaTable.Columns)
                {
                    createTableQuery.Append($"[{column.ColumnName}] {GetSqlDataType(column)}, ");
                }
                createTableQuery.Length -= 2; // Remove the last comma and space
                createTableQuery.Append(")");

                using (SqlConnection destinationConnection = new SqlConnection(destinationConnectionString))
                {
                    destinationConnection.Open();

                    // Check if destination table exists
                    string checkTableExistsQuery = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{destinationTableName}'";
                    SqlCommand checkTableExistsCommand = new SqlCommand(checkTableExistsQuery, destinationConnection);
                    int tableExists = (int)checkTableExistsCommand.ExecuteScalar();

                    if (tableExists > 0)
                    {
                        // Verify if destination table schema matches source table schema
                        string getDestinationSchemaQuery = $"SELECT * FROM {destinationTableName} WHERE 1 = 0";
                        SqlCommand destinationSchemaCommand = new SqlCommand(getDestinationSchemaQuery, destinationConnection);
                        SqlDataAdapter destinationAdapter = new SqlDataAdapter(destinationSchemaCommand);
                        DataTable destinationSchemaTable = new DataTable();
                        destinationAdapter.Fill(destinationSchemaTable);

                        if (!SchemasAreEqual(schemaTable, destinationSchemaTable))
                        {
                            throw new InvalidOperationException("The destination table schema does not match the source table schema.");
                        }

                        // Check if destination table is empty
                        string checkTableEmptyQuery = $"SELECT COUNT(*) FROM {destinationTableName}";
                        SqlCommand checkTableEmptyCommand = new SqlCommand(checkTableEmptyQuery, destinationConnection);
                        int rowCount = (int)checkTableEmptyCommand.ExecuteScalar();

                        if (rowCount > 0)
                        {
                            Console.WriteLine("Warning: The destination table is not empty. Do you want to continue? (yes/no):");
                            string continueResponse = Console.ReadLine();
                            if (!string.Equals(continueResponse, "yes", StringComparison.OrdinalIgnoreCase))
                            {
                                Console.WriteLine("Operation cancelled by user.");
                                return;
                            }
                        }
                    }
                    else
                    {
                        // Create table in destination database
                        SqlCommand createTableCommand = new SqlCommand(createTableQuery.ToString(), destinationConnection);
                        createTableCommand.ExecuteNonQuery();
                    }
                }

                // Get total row count for progress tracking
                string rowCountQuery = $"SELECT COUNT(*) FROM {sourceTableName}";
                SqlCommand rowCountCommand = new SqlCommand(rowCountQuery, sourceConnection);
                int totalRows = (int)rowCountCommand.ExecuteScalar();

                // Copy data from source to destination in batches
                int batchSize = 1000;
                string selectQuery = $"SELECT * FROM {sourceTableName}";
                SqlCommand selectCommand = new SqlCommand(selectQuery, sourceConnection);
                SqlDataAdapter dataAdapter = new SqlDataAdapter(selectCommand);
                DataTable dataTable = new DataTable();

                using (SqlConnection destinationConnection = new SqlConnection(destinationConnectionString))
                {
                    destinationConnection.Open();
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection);
                    bulkCopy.DestinationTableName = destinationTableName;

                    int offset = 0;
                    int rowsTransferred = 0;
                    while (true)
                    {
                        dataTable.Clear();
                        selectCommand.CommandText = $"SELECT * FROM {sourceTableName} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batchSize} ROWS ONLY";
                        dataAdapter.Fill(dataTable);

                        if (dataTable.Rows.Count == 0)
                        {
                            break;
                        }

                        bulkCopy.WriteToServer(dataTable);
                        rowsTransferred += dataTable.Rows.Count;
                        offset += batchSize;

                        // Display progress bar

                        Console.SetCursorPosition(0, Console.CursorTop);
                        Console.Write($"Progress: [{new string('#', rowsTransferred * 20 / totalRows).PadRight(20)}] {rowsTransferred}/{totalRows} rows transferred");
                    }
                }
            }
        }

        static bool SchemasAreEqual(DataTable schemaTable1, DataTable schemaTable2)
        {
            if (schemaTable1.Columns.Count != schemaTable2.Columns.Count)
            {
                return false;
            }

            for (int i = 0; i < schemaTable1.Columns.Count; i++)
            {
                if (schemaTable1.Columns[i].ColumnName != schemaTable2.Columns[i].ColumnName ||
                    schemaTable1.Columns[i].DataType != schemaTable2.Columns[i].DataType)
                {
                    return false;
                }
            }

            return true;
        }

        static string GetSqlDataType(DataColumn column)
        {
            // Map the .NET data type to SQL Server data type
            switch (Type.GetTypeCode(column.DataType))
            {
                case TypeCode.Int32:
                    return "INT";
                case TypeCode.Int64:
                    return "BIGINT";
                case TypeCode.String:
                    return column.MaxLength > 0 ? $"NVARCHAR({column.MaxLength})" : "NVARCHAR(MAX)";
                case TypeCode.DateTime:
                    return "DATETIME";
                case TypeCode.Boolean:
                    return "BIT";
                case TypeCode.Decimal:
                    return "DECIMAL(18, 2)";
                case TypeCode.Double:
                    return "FLOAT";
                default:
                    throw new NotSupportedException($"Data type '{column.DataType.Name}' is not supported.");
            }
        }
    }
}