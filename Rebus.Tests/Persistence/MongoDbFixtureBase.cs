using System.Collections.Generic;
using MongoDB.Driver;
using NUnit.Framework;
using log4net.Config;

namespace Rebus.Tests.Persistence
{
    public abstract class MongoDbFixtureBase
    {
        IMongoDatabase db;
		IMongoClient client;

        static MongoDbFixtureBase()
        {
            XmlConfigurator.Configure();
        }

        public static string ConnectionString
        {
            get { return ConnectionStrings.MongoDb; }
        }

        [SetUp]
        public void SetUp()
        {
            TimeMachine.Reset();

            db = MongoHelper.GetDatabase(ConnectionStrings.MongoDb);
			db.DropDatabase(ConnectionStrings.MongoDb);

			DoSetUp();
        }

        protected virtual void DoSetUp()
        {
        }

        [TearDown]
        public void TearDown()
        {
            DoTearDown();
        }

        protected virtual void DoTearDown()
        {
        }

        protected void DropCollection(string collectionName)
        {
            db.DropCollection(collectionName);
        }

        protected IEnumerable<string> GetCollectionNames()
        {
            return db.ListCollectionNames().ToEnumerable();
        }

        protected IMongoCollection<T> Collection<T>(string collectionName)
        {
            return db.GetCollection<T>(collectionName);
        }
    }
}