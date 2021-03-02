using System;

namespace XmlFileIngestion.Models
{
    public class NodeAttributeCsvRecord
    {
        public Guid NodeId { get; set; }

        public string AttributeName { get; set; }

        public string AttributeValue { get; set; }
    }
}