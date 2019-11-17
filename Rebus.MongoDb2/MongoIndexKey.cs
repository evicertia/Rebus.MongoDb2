using System;
using System.Collections;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;

namespace Rebus.MongoDb2
{
	/// <summary>
	/// Represents a BSON document that can be used where an IMongoIndexKeys is expected.
	/// </summary>
	[BsonSerializer(typeof(MongoIndexKeys.Serializer))]
	public class MongoIndexKeys : BsonDocument
	{
		// constructors
		/// <summary>
		/// Initializes a new instance of the MongoIndexKeys class.
		/// </summary>
		public MongoIndexKeys()
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class specifying whether duplicate element names are allowed
		/// (allowing duplicate element names is not recommended).
		/// </summary>
		/// <param name="allowDuplicateNames">Whether duplicate element names are allowed.</param>
		public MongoIndexKeys(bool allowDuplicateNames)
			: base(allowDuplicateNames)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds one element.
		/// </summary>
		/// <param name="element">An element to add to the document.</param>
		public MongoIndexKeys(BsonElement element)
			: base(element)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		public MongoIndexKeys(Dictionary<string, object> dictionary)
			: base(dictionary)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		/// <param name="keys">A list of keys to select values from the dictionary.</param>
		[Obsolete("Use IndexKeysDocument<IEnumerable<BsonElement> elements) instead.")]
		public MongoIndexKeys(Dictionary<string, object> dictionary, IEnumerable<string> keys)
			: base(dictionary, keys)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		public MongoIndexKeys(IEnumerable<KeyValuePair<string, object>> dictionary)
			: base(dictionary)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		/// <param name="keys">A list of keys to select values from the dictionary.</param>
		[Obsolete("Use IndexKeysDocument<IEnumerable<BsonElement> elements) instead.")]
		public MongoIndexKeys(IDictionary<string, object> dictionary, IEnumerable<string> keys)
			: base(dictionary, keys)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		public MongoIndexKeys(IDictionary dictionary)
			: base(dictionary)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a dictionary of key/value pairs.
		/// </summary>
		/// <param name="dictionary">A dictionary to initialize the document from.</param>
		/// <param name="keys">A list of keys to select values from the dictionary.</param>
		[Obsolete("Use IndexKeysDocument<IEnumerable<BsonElement> elements) instead.")]
		public MongoIndexKeys(IDictionary dictionary, IEnumerable keys)
			: base(dictionary, keys)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds new elements from a list of elements.
		/// </summary>
		/// <param name="elements">A list of elements to add to the document.</param>
		public MongoIndexKeys(IEnumerable<BsonElement> elements)
			: base(elements)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and adds one or more elements.
		/// </summary>
		/// <param name="elements">One or more elements to add to the document.</param>
		[Obsolete("Use IndexKeysDocument<IEnumerable<BsonElement> elements) instead.")]
		public MongoIndexKeys(params BsonElement[] elements)
			: base(elements)
		{
		}

		/// <summary>
		/// Initializes a new instance of the IndexKeysDocument class and creates and adds a new element.
		/// </summary>
		/// <param name="name">The name of the element to add to the document.</param>
		/// <param name="value">The value of the element to add to the document.</param>
		public MongoIndexKeys(string name, BsonValue value)
			: base(name, value)
		{
		}

		// nested classes
		internal class Serializer : SerializeAsNominalTypeSerializer<MongoIndexKeys, BsonDocument>
		{
		}
	}
}
