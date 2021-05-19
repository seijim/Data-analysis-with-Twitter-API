using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Tweetinvi;
using Parquet.Data;
using Parquet;
using Azure.Storage.Blobs;

namespace TwitterStreamApiConsole
{
    class Program
    {
        private static readonly int maxCount = 5;
        private static int counter;
        private static DateTime dt;
        private static string blobFileName;

        static void Main(string[] args)
        {
            Console.WriteLine($"***** Stream started. {DateTime.UtcNow}");
            dt = DateTime.UtcNow;
            blobFileName = $"./tweetdata/{dt.ToString("yyyy")}/{dt.ToString("MM")}/{dt.ToString("dd")}/{dt.ToString("HH")}/tw_{Guid.NewGuid().ToString("D")}.parquet";

            counter = 0;
            var tweets = new TweetsEntity();
            tweets.CreatedAt = new List<DateTimeOffset>();
            tweets.CreatedBy = new List<string>();
            tweets.Source = new List<string>();
            tweets.Text = new List<string>();

            StartFilteredStream(tweets);

            Console.ReadLine();
        }

        private static async void StartFilteredStream(TweetsEntity tweets)
        {
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

                    tweets.CreatedAt.Add(args.Tweet.CreatedAt);
                    tweets.CreatedBy.Add(args.Tweet.CreatedBy.ToString());
                    tweets.Source.Add(args.Tweet.Source);
                    tweets.Text.Add(args.Tweet.Text);
                }
                ++counter;
                if (counter > maxCount)
                {
                    stream.Stop();
                }
            };
            await stream.StartMatchingAllConditionsAsync();

            Console.WriteLine("***** Stream stopped.");
            await CreateParquetFile(tweets);
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
            var connectionString = "<your ADLS Gen2 storage connection string>";
            var blobServiceClient = new BlobServiceClient(connectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient("<your filesystem name>");

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
