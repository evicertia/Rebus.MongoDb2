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
        const string DueLockProperty = "due_lock";
        const int RebusDueTimeoutSchedulerTimerIntervalInMs = 300;
        readonly IMongoCollection<BsonDocument> collection;
        readonly TimeSpan _lockTimeoutsOffset;
        readonly int _maxDueTimeoutsRetrieved;

        /// <summary>
        /// Constructs the timeout storage, connecting to the Mongo database pointed to by the given connection string,
        /// storing the timeouts in the given collection with specified lockTimeoutsOffset
        /// </summary>
        public MongoDbTimeoutStorage(string connectionString, string collectionName, TimeSpan? lockTimeoutsOffset = null, int? maxDueTimeoutsRetrieved = null)
        {
            _lockTimeoutsOffset = lockTimeoutsOffset.GetValueOrDefault(TimeSpan.FromSeconds(5));

            if (_lockTimeoutsOffset.TotalMilliseconds <= RebusDueTimeoutSchedulerTimerIntervalInMs)
            {
                var message = $"{nameof(lockTimeoutsOffset)} must be greater than {RebusDueTimeoutSchedulerTimerIntervalInMs} ms in order to {nameof(MongoDbTimeoutStorage)} locking " +
                            $" feature work properly (see Rebus.Bus.DueTimeoutScheduler internal timer interval = { RebusDueTimeoutSchedulerTimerIntervalInMs }) ms";
                throw new ArgumentException(message);
            }

            _maxDueTimeoutsRetrieved = maxDueTimeoutsRetrieved.GetValueOrDefault(5);

            if (_maxDueTimeoutsRetrieved <= 0)
                throw new ArgumentException("maxRetrievedTimeouts must be greater than 0");

            var database = MongoHelper.GetDatabase(connectionString);
            collection = database.GetCollection<BsonDocument>(collectionName);

			var indexBuilder = Builders<BsonDocument>.IndexKeys;
			var indexModel = new CreateIndexModel<BsonDocument>(indexBuilder.Ascending(TimeProperty).Ascending(DueLockProperty), new CreateIndexOptions() { Background = true, Unique = false });
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
                .Add(ReplyToProperty, (BsonValue)newTimeout.ReplyTo ?? BsonNull.Value)
                .Add(DueLockProperty, BsonNull.Value);

            collection.InsertOne(doc);
        }

        /// <summary>
        /// Gets all timeouts that are due by now. Doesn't remove the timeouts but locks them for a few seconds,
        /// each individual timeout can be marked as processed by calling <see cref="DueTimeout.MarkAsProcessed"/>
        /// </summary>
        public DueTimeoutsResult GetDueTimeouts()
        {
			var dueTimeouts = new List<DueTimeout>();
			var currentDate = RebusTimeMachine.Now();
            var dueTimeoutFilter = Builders<BsonDocument>.Filter.Lte(TimeProperty, currentDate);
            var unlockedTimeoutFilter = Builders<BsonDocument>.Filter.Or(
                Builders<BsonDocument>.Filter.Eq(DueLockProperty, BsonNull.Value),
                Builders<BsonDocument>.Filter.Lt(DueLockProperty, currentDate)
            );
            var filter = dueTimeoutFilter & unlockedTimeoutFilter;
            var dueLockShifted = currentDate + _lockTimeoutsOffset;
            var update = Builders<BsonDocument>.Update.Set(DueLockProperty, dueLockShifted);
            var sort = Builders<BsonDocument>.Sort.Ascending(TimeProperty).Ascending(DueLockProperty);
            var options = new FindOneAndUpdateOptions<BsonDocument> { Sort = sort };

            for (var i = 0; i < _maxDueTimeoutsRetrieved; i++)
            {
                var doc = collection.FindOneAndUpdate(filter, update, options);

                if (doc == null)
                    break;

                var timeout = new DueMongoTimeout(GetString(doc, ReplyToProperty),
                    GetString(doc, CorrIdProperty),
                    doc[TimeProperty].ToUniversalTime(),
                    GetGuid(doc, SagaIdProperty),
                    GetString(doc, DataProperty),
                    collection,
                    (ObjectId)doc[IdProperty]);

                dueTimeouts.Add(timeout);
            }

            return new DueTimeoutsResult(dueTimeouts);
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