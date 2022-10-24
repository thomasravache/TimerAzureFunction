using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimerAzureFunction
{
    internal class ConnectionDb
    { 
        public readonly SqlConnection Connection;
        public ConnectionDb(string connectionString)
        {
            Connection = GetConnection(connectionString);
        }

        private SqlConnection GetConnection(string connectionString)
        {
            try
            {
                var connection = new SqlConnection(connectionString);
                connection.Open();

                Console.WriteLine("Conexão estabelecida");

                return connection;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro na conexão");
                Console.WriteLine(ex.Message);

                throw;
            }
        }

        public void ExecuteQuery(string query)
        {
            try
            {
                var sqlQuery = new SqlCommand(query, Connection);

                sqlQuery.ExecuteNonQuery();

                Console.WriteLine("Query efetuada com sucesso");
            }
            catch
            {
                Console.WriteLine("Erro ao executar comando");

                throw;
            }
        }

        //public void ReadData(string query)
        //{
        //    using (var reader = new SqlCommand(query, _connection).ExecuteReader())
        //    {
        //        while (reader.Read())
        //        {
        //            Console.WriteLine(reader["Titulo"].ToString());
        //        }
        //    }
        //}

        public void CloseConnection()
        {
            Connection.Close();
        }
    }
}
