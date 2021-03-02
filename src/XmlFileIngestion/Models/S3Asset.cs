namespace XmlFileIngestion.Models
{
    public class S3Asset
    {
        public string AssetId { get; set; }

        public string BucketName { get; set; }

        public string Key { get; set; }

        public string Base64Content { get; set; }
    }
}