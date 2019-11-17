﻿using System;

using MongoDB.Driver;

namespace Rebus.MongoDb2
{
    internal static class MongoHelper
    {
        public static IMongoDatabase GetDatabase(string connectionString)
        {
            var mongoUrl = new MongoUrl(connectionString);
            var databaseName = mongoUrl.DatabaseName;
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentException(
                    "Expected that the connection string would be qualified with a database name!",
                    "connectionString");
            }

            var client = new MongoClient(mongoUrl);

            return client.GetDatabase(databaseName);
        }
	}
}