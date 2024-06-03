using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ByODBCDriver
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int batchSize = 50000;
            string connectionString = "DSN=DatabricksWarehouse";

            string query = "SELECT c.customer_id, c.customer_name, c.customer_email, o.order_id, o.product, o.amount" +
                " FROM Customers c" +
                " JOIN Orders o ON c.customer_id = o.customer_id";

            //InsertData(connectionString, batchSize);
            GetData(connectionString, query);
        }

        public static List<string> GetData(string connectionString, string query)
        {
            var before = DateTime.Now;
            var after = DateTime.Now;
            List<string> records = new List<string>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    before = DateTime.Now;
                    Console.WriteLine("Ready to open ODBC connection:   " + DateTime.Now.ToString());
                    connection.Open();

                    using (OdbcCommand command = new OdbcCommand(query, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string record = reader.GetString(0) + " " + reader.GetString(1) +
                                    " " + reader.GetString(2) + " " + reader.GetString(3) +
                                    " " + reader.GetString(4) + " " + reader.GetString(5);
                                records.Add(record);
                            }
                        }
                    }
                    after = DateTime.Now;
                    Console.WriteLine("records.Count():  " + records.Count().ToString());
                    Console.WriteLine("Total elapsed time to join and retrieve " + records.Count().ToString() + "   records from Databricks VIA ODBC:   " + after.Subtract(before).ToString());
                    Console.WriteLine("records[9999]:  " + records[50]);
                    Console.ReadLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    Console.ReadLine();
                }
            }

            return records;
        }

        public static void InsertData(string connectionString, int batchSize)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    Console.WriteLine("Connection opened successfully.");
                    StringBuilder insertQuery = new StringBuilder();
                    insertQuery.Append("INSERT INTO maindev.default.OdbcWrittenCustomers (CustomerId, CustomerName, CustomerEmail, CustomerAddress) VALUES ");

                    for (int i = 1; i <= 1000000; i++)
                    {
                        insertQuery.Append($"({i}, 'Customer {i}', 'customer{i}@example.com', 'Address {i}'),");

                        if (i % batchSize == 0 || i == 1000000)
                        {
                            insertQuery.Length--; // Remove the trailing comma
                            insertQuery.Append(";");

                            using (OdbcCommand insertCmd = new OdbcCommand(insertQuery.ToString(), connection))
                            {
                                insertCmd.ExecuteNonQuery();
                            }

                            insertQuery.Clear();
                            insertQuery.Append("INSERT INTO maindev.default.OdbcWrittenCustomers (CustomerId, CustomerName, CustomerEmail, CustomerAddress) VALUES ");

                            if (i % batchSize == 0)
                            {
                                Console.WriteLine($"{i} records inserted.");
                            }
                        }
                    }

                    Console.WriteLine("All records inserted successfully.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred: {ex.Message}");
                    Console.ReadLine();
                }
            }
        }
    }
}
