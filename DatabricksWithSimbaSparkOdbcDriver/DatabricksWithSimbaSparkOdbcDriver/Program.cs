using System.Data.Odbc;
using Dapper;

namespace DatabricksWithSimbaSparkOdbcDriver;

internal class Program
{
    public static void Main(string[] args)
    {
        OdbcConnectionStringBuilder odbcConnectionStringBuilder = new()
        {
            Driver = "Simba Spark ODBC Driver"
        };
        odbcConnectionStringBuilder.Add("Host", "<server-hostname>");
        odbcConnectionStringBuilder.Add("Port", "443");
        odbcConnectionStringBuilder.Add("SSL", "1");
        odbcConnectionStringBuilder.Add("ThriftTransport", "2");
        odbcConnectionStringBuilder.Add("AuthMech", "3");
        odbcConnectionStringBuilder.Add("UID", "token");
        odbcConnectionStringBuilder.Add("PWD", "<personal-access-token>");
        odbcConnectionStringBuilder.Add("HTTPPath", "<http-path>");

        RunAdoNet(odbcConnectionStringBuilder.ConnectionString);

        RunDapper(odbcConnectionStringBuilder.ConnectionString);
    }

    private static void RunAdoNet(string connectionString)
    {
        // Create a new OdbcConnection object with the connection string
        using var connection = new OdbcConnection(connectionString);

        try
        {
            // Open the connection
            connection.Open();

            const string query = "SELECT * FROM my_table";

            // Create a new OdbcCommand object
            using var command = new OdbcCommand(query, connection);

            // Execute the command and get a reader
            using var reader = command.ExecuteReader();

            // Read the data from the reader
            while (reader.Read())
            {
                // Do something with the data
                Console.WriteLine(reader.GetString(0));
            }

            // Close the reader and connection
            reader.Close();
            connection.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    private static void RunDapper(string connectionString)
    {
        // Create a new OdbcConnection object with the connection string
        using var connection = new OdbcConnection(connectionString);

        try
        {
            // Open the connection
            connection.Open();

            const string query = "SELECT * FROM my_table";

            var results = connection.Query<dynamic>(query);

            foreach (var result in results)
            {
                // Access result properties here
                Console.WriteLine(result.ColumnName);
            }
        }
        catch (OdbcException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}