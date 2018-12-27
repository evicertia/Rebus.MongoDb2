using System.Configuration;
using System.Linq;
using NUnit.Framework;

namespace Rebus.Tests.Persistence
{
    public static class ConnectionStrings
    {
        public static string MongoDb
        {
            get { return "mongodb://localhost:27017/rebus_test"; }
        }
    }
}