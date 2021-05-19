using System;
using System.Threading.Tasks;
using Tweetinvi;

namespace TwitterStreamApiConsole
{
    class Program
    {
        private static readonly int maxCount = 5;
        private static int counter;

        static void Main(string[] args)
        {
            counter = 0;

            // Start Twitter Stream reading with Filtered Stream API
            StartFilteredStream().Wait();

            Console.ReadLine();
        }

        /// <summary>
        /// Twitter Filtered Stream API
        /// Nuget Package : Tweetinvi
        /// https://linvi.github.io/tweetinvi/dist/intro/basic-concepts.html#twitterclient
        /// </summary>
        private static async Task StartFilteredStream()
        {
            Console.WriteLine($"***** Stream started. {DateTime.UtcNow}");

            // User client & stream
            var client = new TwitterClient("<your API Key>", "<your API Secret>",
                                             "<your Access Token>", "<your Access Token Secret>");
            var stream = client.Streams.CreateFilteredStream();

            // Add filters
            stream.AddTrack("コロナ");
            stream.AddTrack("大変");

            // Read stream
            stream.MatchingTweetReceived += (sender, args) =>
            {
                var lang = args.Tweet.Language;
                // Specify Japanese & Remove bot tweets
                if (lang == Tweetinvi.Models.Language.Japanese && args.Tweet.Source.Contains(">Twitter "))  
                {
                    Console.WriteLine("----------------------------------------------------------------------");
                    Console.WriteLine($"** CreatedAt : {args.Tweet.CreatedAt}");
                    Console.WriteLine($"** CreatedBy : {args.Tweet.CreatedBy}");
                    Console.WriteLine($"** Source    : {args.Tweet.Source}");
                    Console.WriteLine($"** Text      : {args.Tweet.Text}");
                }
                ++counter;
                if (counter >= maxCount)
                {
                    stream.Stop();
                }
            };
            await stream.StartMatchingAllConditionsAsync();

            Console.WriteLine();
            Console.WriteLine($"***** Stream stopped. {DateTime.UtcNow} (counter : {counter})");
        }
    }
}
