using MongoDB.Driver;

namespace Rebus.Tests.Persistence
{
	public class MongoHelper
	{
		public static IMongoDatabase GetDatabase(string connectionString)
		{
			var mongoUrl = new MongoUrl(connectionString);

			return new MongoClient(mongoUrl)
				.GetDatabase(mongoUrl.DatabaseName);
		}
	}

	public static class MongoExtensions
	{
		public static void DropDatabase(this IMongoDatabase db, string connectionString)
		{
			var databaseName = new MongoUrl(connectionString).DatabaseName;
			db.Client.DropDatabase(databaseName);
		}
	}
}