using System;
using MongoDB.Driver;

namespace Rebus.Tests.Persistence
{
	public class MongoHelper
	{
		public static IMongoDatabase GetDatabase(string connectionString)
		{
			var url = new MongoUrl(connectionString);
			var settings = MongoClientSettings.FromConnectionString(connectionString);
			settings.SdamLogFilename = "/tmp/mongo.log";

			return new MongoClient(settings)
				.GetDatabase(url.DatabaseName);
		}
	}

	public static class MongoExtensions
	{
		public static void DropDatabase(this IMongoDatabase db, string connectionString)
		{
			try
			{
				var databaseName = new MongoUrl(connectionString).DatabaseName;
				db.Client.DropDatabase(databaseName);
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.ToString());
				throw;
			}
		}
	}
}