using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportCSVDemo
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string path = Environment.CurrentDirectory;
            string csvPath = path.Replace(@"bin\Debug", @"ArchiveData\customer_100MB.csv");

            Stopwatch stopwatch = new Stopwatch();

            stopwatch.Start();
            var dataFromCSV = GetDataTabletFromCSVFile(csvPath);
            using (SqlConnection conn = new SqlConnection(@"Data Source=(localdb)\mssqllocaldb;Initial Catalog=ImportCSV;Integrated Security=True"))
            {
                conn.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "dbo.customer";

                    Parallel.ForEach(dataFromCSV.Columns.Cast<object>(),
                        currentElement =>
                        {
                            bulkCopy.ColumnMappings.Add(currentElement.ToString(), currentElement.ToString());
                        });

                    bulkCopy.WriteToServer(dataFromCSV);
                }
                conn.Close();
            }
            stopwatch.Stop();
            Console.WriteLine("Done importing customers!");
            Console.WriteLine("Elapsed Time is {0} s", stopwatch.ElapsedMilliseconds/1000);
            Console.ReadLine();
        }
        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                return null;
            }
            return csvData;
        }
    }
}
