using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ByODBCDriver
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Connection string for the Delta Lake ODBC driver
            //string connectionString = "Driver=Simba Spark;Host=https://adb-2986225719779619.19.azuredatabricks.net;Port=443;Schema=default;SSL=1;AuthMech=3;HTTPPath=sql/protocolv1/o/2986225719779619/0313-132056-j7llecvq;AuthMech=3;UID=token;PWD=435efe65ee20bc3f";

           /* string connectionString = "DSN=SimbaSpark;" +
                          "Host=https://adb-2986225719779619.19.azuredatabricks.net;" +
                          "Port=443;" +
                          "HTTPPath=sql/protocolv1/o/2986225719779619/0313-132056-j7llecvq;" +
                          "ThriftTransport=2;" +
                          "SSL=1;" +
                          "AuthMech=3;" +
                          "UID=token;" +
                          "PWD=435efe65ee20bc3f";
            */
            string connectionString = "DSN=DatabricksWarehouse";



            // SQL query to select data from Delta Lake table
            string query = "SELECT c.customer_id, c.customer_name, c.customer_email, o.order_id, o.product, o.amount" +
                " FROM Customers c" +
                " JOIN Orders o ON c.customer_id = o.customer_id";
            var before = DateTime.Now;
            var after = DateTime.Now;
            List<string> records = new List<string>();
            // Create the ODBC connection
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    before = DateTime.Now;
                    
                    Console.WriteLine("Ready to open ODBC connection:   " + DateTime.Now.ToString());
                    // Open the connection
                    connection.Open();

                    // Create the command
                    using (OdbcCommand command = new OdbcCommand(query, connection))
                    {
                        // Execute the command and get the data
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                // Access data using reader
                                // Console.WriteLine(reader.GetString(0)); // Example: assuming the first column is a string

                               // object[] currentRow = new object[reader.FieldCount];  // Array to store the record

                               // reader.GetValues(currentRow);   // Populate the array with column values

                                string record = reader.GetString(0) + " " + reader.GetString(1) +
                                    " " + reader.GetString(2) + " " + reader.GetString(3) +
                                    " " + reader.GetString(4) + " " + reader.GetString(5);  // Index 0 assumes that you are reading the first column

                                // Add the record to the list
                                records.Add(record);
                            }
                            
                        }
                    }
                    after = DateTime.Now;
                    Console.WriteLine("records.Count():  " + records.Count().ToString());
                    Console.WriteLine("Total elapsed time to join and retrieve " + records.Count().ToString() + "records from Databricks VIA ODBC:   " + after.Subtract(before).ToString());
                    Console.WriteLine("records[9999]:  " + records[50]);
                    Console.ReadLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    Console.ReadLine();
                }
            }
        }
    }
}
