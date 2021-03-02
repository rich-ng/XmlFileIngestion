using System;

namespace XmlFileIngestion.Models
{
    public class NodeCsvRecord
    {
        public string AssetId { get; set; }

        public Guid NodeId { get; set; }

        public string Name { get; set; }

        public string XPath { get; set; }

        public bool HasText { get; set; }
    }
}