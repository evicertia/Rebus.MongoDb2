using System;
using System.Linq;
using System.Collections.Generic;

using MongoDB.Bson;
using MongoDB.Driver;

using Rebus.Timeout;

namespace Rebus.MongoDb2
{
    /// <summary>
    /// Implementation of <see cref="IStoreTimeouts"/> that stores timeouts in a MongoDB
    /// </summary>
    public class MongoDbTimeoutStorage : IStoreTimeouts
    {
        const string ReplyToProperty = "reply_to";
        const string CorrIdProperty = "corr_id";
        const string TimeProperty = "time";
        const string SagaIdProperty = "saga_id";
        const string DataProperty = "data";
        const string IdProperty = "_id";
        readonly IMongoCollection<BsonDocument> collection;

        /// <summary>
        /// Constructs the timeout storage, connecting to the Mongo database pointed to by the given connection string,
        /// storing the timeouts in the given collection
        /// </summary>
        public MongoDbTimeoutStorage(string connectionString, string collectionName)
        {
            var database = MongoHelper.GetDatabase(connectionString);
            collection = database.GetCollection<BsonDocument>(collectionName);

			var indexBuilder = Builders<BsonDocument>.IndexKeys;
			var indexModel = new CreateIndexModel<BsonDocument>(indexBuilder.Ascending(TimeProperty), new CreateIndexOptions() { Background = true, Unique = false });
			collection.Indexes.CreateOne(indexModel);
        }

        /// <summary>
        /// Adds the timeout to the underlying collection of timeouts
        /// </summary>
        public void Add(Timeout.Timeout newTimeout)
        {
            var doc = new BsonDocument()
                .Add(CorrIdProperty, (BsonValue) newTimeout.CorrelationId ?? BsonNull.Value )
                .Add(SagaIdProperty, newTimeout.SagaId)
                .Add(TimeProperty, newTimeout.TimeToReturn)
                .Add(DataProperty, (BsonValue) newTimeout.CustomData ?? BsonNull.Value)
                .Add(ReplyToProperty, (BsonValue) newTimeout.ReplyTo ?? BsonNull.Value);

            collection.InsertOne(doc);
        }

        /// <summary>
        /// Gets all timeouts that are due by now. Doesn't remove the timeouts or change them or anything,
        /// each individual timeout can be marked as processed by calling <see cref="DueTimeout.MarkAsProcessed"/>
        /// </summary>
        public DueTimeoutsResult GetDueTimeouts()
        {
            var result = collection.Find(Builders<BsonDocument>.Filter.Lte(TimeProperty, RebusTimeMachine.Now()))
                                   .Sort(Builders<BsonDocument>.Sort.Ascending(TimeProperty));

			return new DueTimeoutsResult(result.Project(r =>
					new DueMongoTimeout(GetString(r, ReplyToProperty),
						GetString(r, CorrIdProperty),
						r[TimeProperty].ToUniversalTime(),
						GetGuid(r, SagaIdProperty),
						GetString(r, DataProperty),
						collection,
						(ObjectId) r[IdProperty])
					).ToList());
        }

        static Guid GetGuid(BsonDocument doc, string propertyName)
        {
            return doc.Contains(propertyName) ? doc[propertyName].AsGuid : Guid.Empty;
        }

        static string GetString(BsonDocument doc, string propertyName)
        {
            return doc.Contains(propertyName) ? (doc[propertyName] != BsonNull.Value ? doc[propertyName].AsString : null) : "";
        }

        class DueMongoTimeout : DueTimeout
        {
            readonly IMongoCollection<BsonDocument> collection;
            readonly ObjectId objectId;

            public DueMongoTimeout(string replyTo, string correlationId, DateTime timeToReturn, Guid sagaId, string customData, IMongoCollection<BsonDocument> collection, ObjectId objectId)
                : base(replyTo, correlationId, timeToReturn, sagaId, customData)
            {
                this.collection = collection;
                this.objectId = objectId;
            }

            public override void MarkAsProcessed()
            {
                collection.DeleteOne(Builders<BsonDocument>.Filter.Eq(IdProperty, objectId));
            }
        }
    }
}