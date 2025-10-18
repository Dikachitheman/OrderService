using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using OrderService.Model;

namespace OrderService.Model
{
    public class OrderContext : DbContext
    {
        public DbSet<Order> Orders { get; set; } = null!;

        public OrderContext(DbContextOptions<OrderContext> options) : base(options)
        {
        }
    }
}
