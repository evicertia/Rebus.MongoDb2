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
        readonly TimeSpan _lockTimeoutsOffset = new TimeSpan(0,0,2);
        readonly int _maxDueTimeoutsRetrieved = 4;

        /// <summary>
        /// Constructs the timeout storage, connecting to the Mongo database pointed to by the given connection string,
        /// storing the timeouts in the given collection with specified lockTimeoutsOffset
        /// </summary>
        public MongoDbTimeoutStorage(string connectionString, string collectionName, TimeSpan? lockTimeoutsOffset = null, int? maxDueTimeoutsRetrieved = null)
        {
            if (lockTimeoutsOffset == null)
                lockTimeoutsOffset = _lockTimeoutsOffset;

            if (lockTimeoutsOffset.Value.TotalMilliseconds <= RebusDueTimeoutSchedulerTimerIntervalInMs)
            {
                var message=$"{nameof(lockTimeoutsOffset)} must be greater than {RebusDueTimeoutSchedulerTimerIntervalInMs} ms in order to {nameof(MongoDbTimeoutStorage)} locking " +
                            $" feature work properly (see Rebus.Bus.DueTimeoutScheduler internal timer interval = { RebusDueTimeoutSchedulerTimerIntervalInMs }) ms";
                throw new ArgumentException(message);
            }

            _lockTimeoutsOffset = lockTimeoutsOffset.Value;

            if (maxDueTimeoutsRetrieved == null)
                maxDueTimeoutsRetrieved = _maxDueTimeoutsRetrieved;

            if (maxDueTimeoutsRetrieved <= 0)
                throw new ArgumentException("maxRetrievedTimeouts must be greater than 0");

           _maxDueTimeoutsRetrieved = maxDueTimeoutsRetrieved.Value;

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
        /// Gets all timeouts that are due by now. Doesn't remove the timeouts or change them or anything,
        /// each individual timeout can be marked as processed by calling <see cref="DueTimeout.MarkAsProcessed"/>
        /// </summary>
        public DueTimeoutsResult GetDueTimeouts()
        {
			var dueTimeouts = new List<DueTimeout>();
			var currentDate = RebusTimeMachine.Now();
            var dueTimeoutFilter = Builders<BsonDocument>.Filter.Lte(TimeProperty,currentDate);
            var unlockedTimeoutFilter = Builders<BsonDocument>.Filter.Eq(DueLockProperty, BsonNull.Value) |
                                Builders<BsonDocument>.Filter.Lt(DueLockProperty, currentDate);

            var timeoutReadyToBeFiredFilter = dueTimeoutFilter & unlockedTimeoutFilter;

            var dueLockShifted = currentDate + _lockTimeoutsOffset;

            for (var i = 0; i < _maxDueTimeoutsRetrieved; i++)
            {
                var findOneAndUpdateOptions = new FindOneAndUpdateOptions<BsonDocument> { Sort = Builders<BsonDocument>.Sort.Ascending(TimeProperty).Ascending(DueLockProperty) };
                var r = collection.FindOneAndUpdate(timeoutReadyToBeFiredFilter, Builders<BsonDocument>.Update.Set(DueLockProperty, dueLockShifted), findOneAndUpdateOptions);

                if (r == null)
                    break;

                var timeout = new DueMongoTimeout(GetString(r, ReplyToProperty),
                    GetString(r, CorrIdProperty),
                    r[TimeProperty].ToUniversalTime(),
                    GetGuid(r, SagaIdProperty),
                    GetString(r, DataProperty),
                    collection,
                    (ObjectId)r[IdProperty]);

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