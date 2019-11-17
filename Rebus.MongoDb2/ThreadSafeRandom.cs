using System;
using System.Threading;

namespace Rebus.MongoDb2
{
    public static class ThreadSafeRandom
    {
        private static readonly Random GlobalRandom = new Random();
        private static readonly ThreadLocal<Random> LocalRandom = new ThreadLocal<Random>(() =>
        {
            lock (GlobalRandom)
            {
                return new Random(GlobalRandom.Next());
            }
        });

        public static int Next(int min = 0, int max = Int32.MaxValue)
        {
            return LocalRandom.Value.Next(min, max);
        }

        public static double Next(double min, double max = double.MaxValue)
        {
            Random random = new Random();
            double sample = random.NextDouble();
            return (max * sample) + (min * (1d - sample));
        }
    }
}