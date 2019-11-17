using System;

using MongoDB.Bson;

namespace Rebus.MongoDb2
{
    /// <summary>
    /// Represents information about an index.
    /// </summary>
    public class MongoIndex
    {
        // private fields
        private BsonDocument _document;

        // constructors
        /// <summary>
        /// Creates a new instance of the IndexInfo class.
        /// </summary>
        /// <param name="document">The BSON document that contains information about the index.</param>
        public MongoIndex(BsonDocument document)
        {
            _document = document;
        }

        // public properties
        /// <summary>
        /// Gets a value indicating whether dups were dropped when the index was created.
        /// </summary>
        public bool DroppedDups
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("dropDups", out value))
                {
                    return value.ToBoolean();
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the index was created in the background.
        /// </summary>
        public bool IsBackground
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("background", out value))
                {
                    return value.ToBoolean();
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the index is sparse.
        /// </summary>
        public bool IsSparse
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("sparse", out value))
                {
                    return value.ToBoolean();
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the index is unique.
        /// </summary>
        public bool IsUnique
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("unique", out value))
                {
                    return value.ToBoolean();
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Gets the key of the index.
        /// </summary>
        public MongoIndexKeys Key
        {
            get
            {
                return new MongoIndexKeys(_document["key"].AsBsonDocument.Elements);
            }
        }

        /// <summary>
        /// Gets the name of the index.
        /// </summary>
        public string Name
        {
            get
            {
                return _document["name"].AsString;
            }
        }

        /// <summary>
        /// Gets the namespace of the collection that the index is for.
        /// </summary>
        public string Namespace
        {
            get
            {
                return _document["ns"].AsString;
            }
        }

        /// <summary>
        /// Gets the raw BSON document containing the index information.
        /// </summary>
        public BsonDocument RawDocument
        {
            get { return _document; }
        }

        /// <summary>
        /// Gets the time to live value (or TimeSpan.MaxValue if index doesn't have a time to live value).
        /// </summary>
        public TimeSpan TimeToLive
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("expireAfterSeconds", out value))
                {
                    return TimeSpan.FromSeconds(value.ToInt32());
                }
                else
                {
                    return TimeSpan.MaxValue;
                }
            }
        }

        /// <summary>
        /// Gets the version of the index.
        /// </summary>
        public int Version
        {
            get
            {
                BsonValue value;
                if (_document.TryGetValue("v", out value))
                {
                    return value.ToInt32();
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}
