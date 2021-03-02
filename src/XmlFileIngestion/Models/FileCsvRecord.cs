namespace XmlFileIngestion.Models
{
    public class FileCsvRecord
    {
        public string AssetId { get; set; }

        public string FileType { get; set; }

        public string BucketName { get; set; }

        public string Key { get; set; }
    }
}