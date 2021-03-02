using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Amazon;
using Amazon.S3;
using CsvHelper;
using CsvHelper.Configuration;
using HtmlAgilityPack;
using Newtonsoft.Json;
using Sgml;
using XmlFileIngestion.Models;

namespace XmlFileIngestion
{
    static class Program
    {
        private const string RootDirectory = "Artifacts";

        private static string AssetDirectory
        {
            get { return Path.Combine(RootDirectory, "Assets"); }
        }

        private static string JsonDirectory
        {
            get { return Path.Combine(RootDirectory, "Json"); }
        }

        private static string CsvDirectory
        {
            get { return Path.Combine(RootDirectory, "Csv"); }
        }

        static async Task Main(string[] args)
        {
            CreateDirectories();

            // Download files from S3
            await DownloadS3Files();

            await CreateJsonFiles();

            await CreateCsvFiles();
        }

        private static void CreateDirectories()
        {
            Directory.CreateDirectory(AssetDirectory);

            Directory.CreateDirectory(JsonDirectory);

            Directory.CreateDirectory(CsvDirectory);
        }

        private static async Task DownloadS3Files()
        {
            IEnumerable<S3Asset> GetS3AssetFromCsv()
            {
                using var reader = new StreamReader("export.csv");
                using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    HasHeaderRecord = true,
                    HeaderValidated = null,
                    MissingFieldFound = null
                });

                return csv.GetRecords<S3Asset>().ToList();
            }

            string GetFilePath(S3Asset s3Asset)
            {
                var fileName = s3Asset.AssetId.ToString();
                return Path.Combine(AssetDirectory, fileName);
            }

            string Base64Encode(string plainText) {
                var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
                return Convert.ToBase64String(plainTextBytes);
            }

            var s3Assets = new ConcurrentBag<S3Asset>(GetS3AssetFromCsv());

            var tasks = Enumerable.Range(0, 10)
                .Select(_ => Task.Run(async () =>
                {
                    var s3Client = new AmazonS3Client(RegionEndpoint.USEast1);

                    while (s3Assets.TryTake(out var s3Asset))
                    {
                        var filePath = GetFilePath(s3Asset);
                        if (File.Exists(filePath))
                        {
                            continue;
                        }

                        var response = await s3Client.GetObjectAsync(s3Asset.BucketName, s3Asset.Key);

                        using var streamReader = new StreamReader(response.ResponseStream);
                        var content = await streamReader.ReadToEndAsync();

                        s3Asset.Base64Content = Base64Encode(content);

                        var json = JsonConvert.SerializeObject(s3Asset, Formatting.Indented);

                        await File.WriteAllTextAsync(filePath, json);
                    }
                }));

            await Task.WhenAll(tasks.ToArray());
        }

        private static async Task CreateJsonFiles()
        {
            string Base64Decode(string base64EncodedData) {
                var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
                return Encoding.UTF8.GetString(base64EncodedBytes);
            }

            XDocument XmlParse(string text)
            {
                using var sr = new StringReader(text);

                // setup SgmlReader
                SgmlReader sgmlReader = new SgmlReader()
                {
                    WhitespaceHandling = WhitespaceHandling.All,
                    CaseFolding = CaseFolding.None,
                    InputStream = sr
                };

                var doc = XDocument.Load(sgmlReader);
                return doc;
            }

            void RemoveEmptyTextNodes(HtmlDocument doc)
            {
                var xpath = "//text()[not(normalize-space())]";
                var emptyNodes = doc.DocumentNode.SelectNodes(xpath);

                if (emptyNodes == null)
                {
                    return;
                }

                foreach (HtmlNode emptyNode in emptyNodes)
                {
                    emptyNode.Remove();
                }
            }

            var files = new ConcurrentBag<string>(Directory.EnumerateFiles(AssetDirectory));

            var tasks = Enumerable
                .Range(0, 10)
                .Select(_ => Task.Run(async () =>
                {
                    while (files.TryTake(out var file))
                    {
                        var fileName = Path.GetFileName(file);
                        var filePath = Path.Combine(JsonDirectory, fileName);

                        if (File.Exists(filePath))
                        {
                            continue;
                        }

                        var s3Asset = JsonConvert.DeserializeObject<S3Asset>(await File.ReadAllTextAsync(file));

                        var content = Base64Decode(s3Asset.Base64Content);

                        var document = new HtmlDocument
                        {
                            OptionOutputOriginalCase = true
                        };
                        document.LoadHtml(content);

                        RemoveEmptyTextNodes(document);

                        foreach (var htmlNode in document
                            .DocumentNode
                            .Descendants()
                            .Where(x => x.NodeType == HtmlNodeType.Element))
                        {
                            htmlNode.SetAttributeValue("__nodeid", Guid.NewGuid().ToString());
                            htmlNode.SetAttributeValue("__xpath", htmlNode.XPath);
                            htmlNode.SetAttributeValue("__hastext", htmlNode.ChildNodes.OfType<HtmlTextNode>().Any().ToString());
                        }

                        foreach (var htmlNode in document.DocumentNode
                            .Descendants()
                            .Where(x => x.NodeType != HtmlNodeType.Element)
                            .ToList())
                        {
                            htmlNode.Remove();
                        }

                        XDocument xDoc = null;
                        try
                        {
                            xDoc = XmlParse(document.DocumentNode.InnerHtml);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(Path.GetFileName(file));

                            continue;
                        }

                        var json = JsonConvert.SerializeXNode(xDoc, Formatting.Indented);

                        await File.WriteAllTextAsync(filePath, json);
                    }
                }));

            await Task.WhenAll(tasks.ToArray());
        }

        private static async Task CreateCsvFiles()
        {
            await CreateFilesCsvAsync();
            await CreateNodesCsvAsync();
            await CreateAttributesCsvAsync();
            await CreateNodeAttributesCsvAsync();
            await CreateNodeParentCsvAsync();
        }

        private static async Task CreateFilesCsvAsync()
        {
            var fileName = "files.csv";
            var filePath = Path.Combine(CsvDirectory, fileName);
            if (File.Exists(filePath))
            {
                return;
            }

            await using var writer = new StreamWriter(filePath);
            await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            csv.WriteHeader<FileCsvRecord>();
            await csv.NextRecordAsync();

            var files = new ConcurrentBag<string>(Directory.EnumerateFiles(AssetDirectory));
            while (files.TryTake(out var file))
            {
                var fileContent = await File.ReadAllTextAsync(file);

                var s3Asset = JsonConvert.DeserializeObject<S3Asset>(fileContent);

                csv.WriteRecord(new FileCsvRecord
                {
                    AssetId = s3Asset.AssetId,
                    BucketName = s3Asset.BucketName,
                    Key = s3Asset.Key,
                    FileType = Path.GetExtension(s3Asset.Key)?.ToLower()
                });
                await csv.NextRecordAsync();
            }
        }

        private static async Task CreateNodesCsvAsync()
        {
            var fileName = "nodes.csv";
            var filePath = Path.Combine(CsvDirectory, fileName);
            if (File.Exists(filePath))
            {
                return;
            }

            await using var writer = new StreamWriter(filePath);
            await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            csv.WriteHeader<NodeCsvRecord>();
            await csv.NextRecordAsync();

            var files = new ConcurrentBag<string>(Directory.EnumerateFiles(JsonDirectory));
            while (files.TryTake(out var file))
            {
                var assetId = Path.GetFileName(file);

                var fileContent = await File.ReadAllTextAsync(file);

                var document = JsonConvert.DeserializeXNode(fileContent);

                var records = document
                    .DescendantNodes()
                    .OfType<XElement>()
                    .Select(x => new NodeCsvRecord
                    {
                        AssetId = assetId,
                        Name = x.Name.LocalName,
                        NodeId = Guid.Parse(x.Attribute("__nodeid").Value),
                        XPath = x.Attribute("__xpath").Value,
                        HasText = bool.Parse(x.Attribute("__hastext").Value)
                    });

                await csv.WriteRecordsAsync(records);
            }
        }

        private static async Task CreateAttributesCsvAsync()
        {
            var fileName = "attributes.csv";
            var filePath = Path.Combine(CsvDirectory, fileName);
            if (File.Exists(filePath))
            {
                return;
            }

            var attributeValuesByName = new Dictionary<string, HashSet<string>>();
            var files = Directory.EnumerateFiles(JsonDirectory);
            foreach (var file in files)
            {
                var fileContent = await File.ReadAllTextAsync(file);

                var document = JsonConvert.DeserializeXNode(fileContent);

                var attributes = document
                    .DescendantNodes()
                    .OfType<XElement>()
                    .SelectMany(x => x.Attributes())
                    .Where(x => !x.Name.LocalName.StartsWith("__"));

                foreach (var attribute in attributes)
                {
                    if (!attributeValuesByName.ContainsKey(attribute.Name.LocalName))
                    {
                        attributeValuesByName.Add(attribute.Name.LocalName, new HashSet<string>());
                    }

                    attributeValuesByName[attribute.Name.LocalName].Add(attribute.Value);
                }
            }

            await using var writer = new StreamWriter(filePath);
            await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            csv.WriteHeader<KeyValuePair<string, string>>();
            await csv.NextRecordAsync();

            await csv.WriteRecordsAsync(attributeValuesByName
                .SelectMany(kvp => kvp
                    .Value
                    .Select(v => new KeyValuePair<string, string>(kvp.Key, v))));
        }

        private static async Task CreateNodeAttributesCsvAsync()
        {
            var fileName = "nodeAttributes.csv";
            var filePath = Path.Combine(CsvDirectory, fileName);
            if (File.Exists(filePath))
            {
                return;
            }

            await using var writer = new StreamWriter(filePath);
            await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            csv.WriteHeader<NodeAttributeCsvRecord>();
            await csv.NextRecordAsync();

            foreach (var file in Directory.EnumerateFiles(JsonDirectory))
            {
                var fileContent = await File.ReadAllTextAsync(file);

                var document = JsonConvert.DeserializeXNode(fileContent);

                var nodeAttributes = document
                    .Descendants()
                    .Where(x => x.Attributes().Any(a => !a.Name.LocalName.StartsWith("__")))
                    .SelectMany(x => x.Attributes()
                        .Where(a => !a.Name.LocalName.StartsWith("__"))
                        .Select(a => new NodeAttributeCsvRecord
                        {
                            NodeId = Guid.Parse(x.Attribute("__nodeid").Value),
                            AttributeName = a.Name.LocalName,
                            AttributeValue = a.Value
                        }))
                    .ToList();

                await csv.WriteRecordsAsync(nodeAttributes);
            }
        }

        private static async Task CreateNodeParentCsvAsync()
        {
            var fileName = "nodeParent.csv";
            var filePath = Path.Combine(CsvDirectory, fileName);
            if (File.Exists(filePath))
            {
                return;
            }

            await using var writer = new StreamWriter(filePath);
            await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            csv.WriteHeader<NodeParentCsvRecord>();
            await csv.NextRecordAsync();

            foreach (var file in Directory.EnumerateFiles(JsonDirectory))
            {
                var fileContent = await File.ReadAllTextAsync(file);

                var document = JsonConvert.DeserializeXNode(fileContent);

                var nodeParents = document
                    .Descendants()
                    .Where(x => x.Parent != null)
                    .Select(x => new NodeParentCsvRecord
                    {
                        NodeId = Guid.Parse(x.Attribute("__nodeid").Value),
                        ParentNodeId = Guid.Parse(x.Parent.Attribute("__nodeid").Value)
                    });

                await csv.WriteRecordsAsync(nodeParents);
            }
        }
    }
}