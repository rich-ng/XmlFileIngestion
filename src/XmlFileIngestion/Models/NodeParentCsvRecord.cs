using System;

namespace XmlFileIngestion.Models
{
    public class NodeParentCsvRecord
    {
        public Guid NodeId { get; set; }

        public Guid ParentNodeId { get; set; }
    }
}