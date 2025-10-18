namespace Shared.Events
{
    public class OrderCreatedEvent
    {
        public int OrderId { get; set; }
        public string Product { get; set; }
        public int Quantity { get; set; }
        public string CustomerName { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    public class InventoryUpdatedEvent
    {
        public string Product { get; set; }
        public int NewStock { get; set; }
        public int QuantityChanged { get; set; }
        public DateTime UpdatedAt { get; set; }
    }
}