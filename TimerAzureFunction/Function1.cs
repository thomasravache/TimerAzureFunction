using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace TimerAzureFunction
{
    public class Function1
    {
        [FunctionName("Function1")]
        public void Run([TimerTrigger("*/10 * * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            CopyFilmsFromLocalDb(log);
        }

        private void CopyFilmsFromLocalDb(ILogger log)
        {
            var filmesLocal = GetFilmesLocalDb(log);

            using var filmesExternalDbConnection = new SqlConnection(Environment.GetEnvironmentVariable("ConnectionStrings:FilmesExternalDb"));

            try
            {
                filmesExternalDbConnection.Open();
                int registrosIncluidos = 0;

                log.LogInformation("Conexão com ExternalDb bem sucedida!");

                var filmesExternal = new List<FilmeExternalDb>();

                using (var reader = new SqlCommand("SELECT * FROM Filmes;", filmesExternalDbConnection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        filmesExternal.Add(new FilmeExternalDb
                        {
                            Titulo = reader["Titulo"].ToString(),
                        });
                    }
                    log.LogInformation("Leitura de registros da tabela de Filmes efetuada com sucesso!");
                }

                filmesLocal.ForEach(filmeLocal =>
                {
                    var filmeJaExistenteNoExternal = filmesExternal.Any(filmeExternal => filmeExternal.Titulo == filmeLocal.Titulo);

                    if (!filmeJaExistenteNoExternal)
                    {
                        var query = @$"
                            INSERT INTO
                                Filmes (Titulo)
                            VALUES
                                ('{filmeLocal.Titulo}');
                        ";

                        var sqlCommand = new SqlCommand(query, filmesExternalDbConnection);
                        sqlCommand.ExecuteNonQuery();

                        log.LogInformation($"Filme {filmeLocal.Titulo} inserido na base externa com sucesso!");
                        registrosIncluidos++;
                    }
                });

                log.LogInformation($"Registros incluídos nesta execução: {registrosIncluidos}");
            }
            catch
            {
                throw;
            }
        }

        private List<FilmeLocalDb> GetFilmesLocalDb(ILogger log)
        {
            using var filmesLocalDbConnection = new SqlConnection(Environment.GetEnvironmentVariable("ConnectionStrings:FilmesLocalDb"));

            try
            {
                filmesLocalDbConnection.Open();
                log.LogInformation("Conexão com LocalDb bem sucedida!");

                var filmes = new List<FilmeLocalDb>();

                using (var reader = new SqlCommand("SELECT * FROM Filmes;", filmesLocalDbConnection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        filmes.Add(new FilmeLocalDb
                        {
                            Titulo = reader["Titulo"].ToString(),
                            Duracao = int.Parse(reader["Duracao"].ToString()),
                            ClassificacaoEtaria = int.Parse(reader["ClassificacaoEtaria"].ToString())
                        });
                    }
                }

                log.LogInformation("Consulta Realizada!");

                return filmes;
            }
            catch
            {
                throw;
            }
        }
    }
}
