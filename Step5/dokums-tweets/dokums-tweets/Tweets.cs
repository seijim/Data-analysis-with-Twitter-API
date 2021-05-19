using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Tweetinvi;
using Parquet.Data;
using Parquet;
using Azure.Storage.Blobs;

namespace dokums_tweets
{
    public static class Tweets
    {
        private static readonly object countLock = new object();
        private static string twitterApiKey;
        private static string twitterApiSecret;
        private static string twitterAccessToken;
        private static string twitterAccessTokenSecret;
        private static string storageConnectionString;
        private static string storageContainerName;
        private static TimeSpan maxTimeSpan = new TimeSpan(1, 0, 0);  // Max lifecycle time per launch (Check 'functionTimeout' in host.json)
        //private static int maxCount = 672;      // 500,000ÅÄ31ÅÄ24
        private static int maxCount = 1000;       // Max number of Tweets retrieved per launch (Default:1000)
        private static int commitInterval = 100;  // Max number of Tweets retrieved per loop (Default:100)
        private static int counter;
        private static DateTime startDt;
        private static string blobFileName;
        private static string filteredKeywords;
        private static bool completed = false;

        [FunctionName("StoreTweetData")]
        public static void Run([TimerTrigger("0 0 0 */1 * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"********** StoreTweetData Function started. **********");

            // Environment Variables
            twitterApiKey = Environment.GetEnvironmentVariable("TwitterApiKey");
            twitterApiSecret = Environment.GetEnvironmentVariable("TwitterApiSecret");
            twitterAccessToken = Environment.GetEnvironmentVariable("TwitterAccessToken");
            twitterAccessTokenSecret = Environment.GetEnvironmentVariable("TwitterAccessTokenSecret");
            storageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
            storageContainerName = Environment.GetEnvironmentVariable("StorageContinerName");
            string twMaxTimeSpanMins = Environment.GetEnvironmentVariable("TwMaxTimeSpanMins");
            if (twMaxTimeSpanMins != null)
                maxTimeSpan = new TimeSpan(int.Parse(twMaxTimeSpanMins), 0, 0);
            string twMaxCount = Environment.GetEnvironmentVariable("TwMaxCount");
            if (twMaxCount != null)
                maxCount = int.Parse(twMaxCount);
            string twCommitInterval = Environment.GetEnvironmentVariable("TwCommitInterval");
            if (twCommitInterval != null)
                commitInterval = int.Parse(twCommitInterval);
            filteredKeywords = Environment.GetEnvironmentVariable("TwKeywords");  // Check whether local.settings.json is UTF8 or not 
            if (filteredKeywords == null)
                throw new ApplicationException("TwKeywords not set");

            // Initialize values
            startDt = DateTime.UtcNow;
            completed = false;
            counter = 0;

            // Start Twitter Stream reading loop
            while (true)
            {
                var dt = DateTime.UtcNow;
                blobFileName = $"./tweetdata/{dt.ToString("yyyy")}/{dt.ToString("MM")}/{dt.ToString("dd")}/{dt.ToString("HH")}/tw_{Guid.NewGuid().ToString("D")}.parquet";

                var tweets = new TweetsEntity();
                tweets.CreatedAt = new List<DateTimeOffset>();
                tweets.CreatedBy = new List<string>();
                tweets.Source = new List<string>();
                tweets.Text = new List<string>();

                StartFilteredStream(tweets, commitInterval, log).Wait();
                if (completed)
                    break;

                Thread.Sleep(1000);
            }

            log.LogInformation($"********** StoreTweetData Function ended. **********");
        }

        private static async Task StartFilteredStream(TweetsEntity tweets, int commitInterval, ILogger log)
        {
            log.LogInformation($"***** Twitter Stream started : {DateTime.UtcNow}");
            var prevCounter = counter;
            Tweetinvi.Streaming.IFilteredStream stream = null;
            TwitterClient client = null;

            while (true)
            {
                try
                {
                    // User client & stream (Get values from Twitter Developer Portal)
                    client = new TwitterClient(twitterApiKey, twitterApiSecret, twitterAccessToken, twitterAccessTokenSecret);
                    stream = client.Streams.CreateFilteredStream();

                    // Add filters
                    log.LogInformation($"***** Filtered Keywords : {filteredKeywords}");
                    var keywords = filteredKeywords.Split(new char[] { ' ', 'Å@' });
                    foreach (var keyword in keywords)
                    {
                        stream.AddTrack(keyword);
                    }

                    // Read stream
                    stream.MatchingTweetReceived += (sender, args) =>
                    {
                        var lang = args.Tweet.Language;
                        //***** Specify Japanese & Remove Bot
                        if (lang == Tweetinvi.Models.Language.Japanese && args.Tweet.Source.Contains(">Twitter "))
                        {
                            log.LogInformation("----------------------------------------------------------------------");
                            log.LogInformation($"** CreatedAt : {args.Tweet.CreatedAt}");
                            log.LogInformation($"** CreatedBy : {args.Tweet.CreatedBy}");
                            log.LogInformation($"** Source    : {args.Tweet.Source}");
                            log.LogInformation($"** Text      : {args.Tweet.Text}");

                            tweets.CreatedAt.Add(args.Tweet.CreatedAt);
                            tweets.CreatedBy.Add(args.Tweet.CreatedBy.ToString());
                            var source = args.Tweet.Source;
                            var position = source.IndexOf(">");
                            source = source.Substring(position + 1);
                            position = source.IndexOf("<");
                            source = source.Substring(0, position);
                            tweets.Source.Add(source);
                            tweets.Text.Add(args.Tweet.Text);
                        }
                        lock (countLock)
                        {
                            ++counter;
                        }
                        if (counter >= maxCount || (DateTime.UtcNow - startDt) >= maxTimeSpan)
                        {
                            stream.Stop();
                            completed = true;
                        }
                        else if ((counter - prevCounter) >= commitInterval)
                        {
                            stream.Stop();
                        }
                    };

                    await stream.StartMatchingAllConditionsAsync();
                    log.LogInformation($"***** Twitter Stream stopped : {DateTime.UtcNow} (counter : {counter})");
                    break;
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("The response ended prematurely."))
                    {
                        stream?.Stop();
                        await Task.Delay(1000);
                        log.LogInformation($"***** Retry to start stream : {DateTime.UtcNow}");
                    }
                    else
                        throw;
                }
            }

            // Write tweet data to blob storage
            await CreateParquetFile(tweets);
            log.LogInformation($"***** Tweet Data stored to Blob : {DateTime.UtcNow}");
        }

        private static async Task CreateParquetFile(TweetsEntity tweets)
        {
            ////////////////////////////////////////////////////////////////////////////////////////
            /// Write Parquet file
            /// https://github.com/aloneguid/parquet-dotnet
            /// https://docs.microsoft.com/ja-jp/azure/storage/blobs/storage-quickstart-blobs-dotnet
            ////////////////////////////////////////////////////////////////////////////////////////

            // create data columns with schema metadata and the data
            var createdAtColumn = new Parquet.Data.DataColumn(
                new DataField<DateTimeOffset>("CreatedAt"),
                tweets.CreatedAt.ToArray()
            );
            var createdByColumn = new Parquet.Data.DataColumn(
                new DataField<string>("CreatedBy"),
                tweets.CreatedBy.ToArray()
            );
            var sourceColumn = new Parquet.Data.DataColumn(
                new DataField<string>("Source"),
                tweets.Source.ToArray()
            );
            var textColumn = new Parquet.Data.DataColumn(
                new DataField<string>("Text"),
                tweets.Text.ToArray()
            );

            // create file schema
            var schema = new Schema(createdAtColumn.Field, createdByColumn.Field, sourceColumn.Field, textColumn.Field);

            // create file
            Stream stream = new MemoryStream();
            using (var parquetWriter = new ParquetWriter(schema, stream))
            {
                // create a new row group in the file
                using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                {
                    groupWriter.WriteColumn(createdAtColumn);
                    groupWriter.WriteColumn(createdByColumn);
                    groupWriter.WriteColumn(sourceColumn);
                    groupWriter.WriteColumn(textColumn);
                }
            }

            // Write to Blob storage
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(storageContainerName);

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient(blobFileName);
            stream.Position = 0;
            await blobClient.UploadAsync(stream);
            stream.Close();
        }

        private class TweetsEntity
        {
            public List<DateTimeOffset> CreatedAt { set; get; }
            public List<string> CreatedBy { set; get; }
            public List<string> Source { set; get; }
            public List<string> Text { set; get; }
        }

    }
}
