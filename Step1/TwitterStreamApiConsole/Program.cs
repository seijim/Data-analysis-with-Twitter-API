using System;
using System.Threading.Tasks;
using Tweetinvi;

namespace TwitterStreamApiConsole
{
    class Program
    {
        private static readonly int maxCount = 100;
        private static int counter;

        static void Main(string[] args)
        {
            counter = 0;

            // Start Twitter Stream reading with Sampled Stream V2 API
            StartSampledStreamV2().Wait();

            Console.ReadLine();
        }

        /// <summary>
        /// Twitter Sampled Stream V2 API
        /// Nuget Package : Tweetinvi
        /// https://linvi.github.io/tweetinvi/dist/intro/basic-concepts.html#twitterclient
        /// </summary>
        private static async Task StartSampledStreamV2()
        {
            Console.WriteLine($"***** Stream started. {DateTime.UtcNow}");

            // Application client & stream
            var bearerToken = "<your Bearer Token>";
            var client = new TwitterClient("<your API Key>", "<your API Secret>", bearerToken);
            var stream = client.StreamsV2.CreateSampleStream();

            // Read stream
            stream.TweetReceived += (sender, args) =>
            {
                var lang = args.Tweet.Lang;
                if (lang.ToLower() == "ja")  // Display only Japanese tweets
                {
                    Console.WriteLine("----------------------------------------------------------------------");
                    Console.WriteLine($"** CreatedAt : {args.Tweet.CreatedAt}");
                    Console.WriteLine($"** Source    : {args.Tweet.Source}");
                    Console.WriteLine($"** Text      : {args.Tweet.Text}");
                }
                ++counter;
                if (counter >= maxCount)
                {
                    stream.StopStream();
                }
            };
            await stream.StartAsync();

            Console.WriteLine();
            Console.WriteLine($"***** Stream stopped. {DateTime.UtcNow} (counter : {counter})");
        }
    }
}
