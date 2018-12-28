using MongoDB.Driver;
using Rebus.MongoDb;

namespace Rebus.Tests.Persistence.Sagas.Factories
{
    public class MongoDbSagaPersisterFactory : ISagaPersisterFactory
    {
        IMongoDatabase db;

        public IStoreSagaData CreatePersister()
        {
            db = MongoHelper.GetDatabase(ConnectionStrings.MongoDb);

			db.DropDatabase(ConnectionStrings.MongoDb);

			return new MongoDbSagaPersister(ConnectionStrings.MongoDb)
                .AllowAutomaticSagaCollectionNames();
        }

        public void Dispose()
        {
			db.DropDatabase(ConnectionStrings.MongoDb);
		}
    }
}