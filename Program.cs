using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.Text;
using OrderService.Model;
using Services;
using Shared.Events;
using Amazon.SQS;
using Amazon;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.AddSecurityDefinition("Bearer", new Microsoft.OpenApi.Models.OpenApiSecurityScheme
    {
        Description = "JWT Authorization header using the Bearer scheme. Enter 'Bearer' [space] and then your token",
        Name = "Authorization",
        In = Microsoft.OpenApi.Models.ParameterLocation.Header,
        Type = Microsoft.OpenApi.Models.SecuritySchemeType.ApiKey,
        Scheme = "Bearer"
    });
    
    c.AddSecurityRequirement(new Microsoft.OpenApi.Models.OpenApiSecurityRequirement
    {
        {
            new Microsoft.OpenApi.Models.OpenApiSecurityScheme
            {
                Reference = new Microsoft.OpenApi.Models.OpenApiReference
                {
                    Type = Microsoft.OpenApi.Models.ReferenceType.SecurityScheme,
                    Id = "Bearer"
                }
            },
            Array.Empty<string>()
        }
    });
});

builder.Services.AddDbContext<OrderContext>(options =>
     options.UseSqlServer("Server=ACER\\SQLEXPRESS;Database=OrdersDB;Trusted_Connection=True;TrustServerCertificate=True")
           .EnableSensitiveDataLogging()
           .EnableDetailedErrors());

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = true,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            ValidIssuer = "yourissuer",
            ValidAudience = "youraudience",
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("supersecretkeyatleast32charslong"))
        };
    });

builder.Services.AddAuthorization();

// Configure AWS SQS Client for Production
builder.Services.AddSingleton<IAmazonSQS>(sp =>
{
    var config = new AmazonSQSConfig 
    { 
        RegionEndpoint = RegionEndpoint.USEast1 // Change to your AWS region
    };
    return new AmazonSQSClient(config);
});

builder.Services.AddSingleton<AWSSQSService>(sp =>
{
    var sqsClient = sp.GetRequiredService<IAmazonSQS>();
    var sqsService = new AWSSQSService(sqsClient);
    
    // Create queues asynchronously
    _ = Task.Run(async () =>
    {
        await sqsService.CreateQueueAsync("order-created");
        await sqsService.CreateQueueAsync("inventory-updated");
    });
    
    return sqsService;
});

builder.Services.AddHttpClient("InventoryService", client =>
{
    client.BaseAddress = new Uri("http://localhost:5003");
});

var app = builder.Build();

app.Urls.Add("http://localhost:5002");

app.UseSwagger();
app.UseSwaggerUI();

app.UseAuthentication();
app.UseAuthorization();

using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<OrderContext>();
    context.Database.EnsureCreated();
}

// Start consuming messages from SQS
var sqsService = app.Services.GetRequiredService<AWSSQSService>();
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();

_ = sqsService.ConsumeMessagesAsync<InventoryUpdatedEvent>("inventory-updated", async (evt) =>
{
    Console.WriteLine($"✅ Inventory Updated: {evt.Product} - New Stock: {evt.NewStock}");
}, lifetime.ApplicationStopping);

app.MapGet("/orders", async (OrderContext context) =>
{
    return await context.Orders.ToListAsync();
}).RequireAuthorization();

app.MapGet("/orders/{id}", async (int id, OrderContext context) =>
{
    var order = await context.Orders.FindAsync(id);
    return order is not null ? Results.Ok(order) : Results.NotFound();
}).RequireAuthorization();

app.MapPost("/orders", async (Order order, OrderContext context, IHttpClientFactory httpClientFactory, HttpContext httpContext, AWSSQSService sqsService) =>
{
    var token = httpContext.Request.Headers["Authorization"].ToString().Replace("Bearer ", "");
    
    var client = httpClientFactory.CreateClient("InventoryService");
    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
    
    var inventoryResponse = await client.GetAsync($"/inventory/{order.Product}");
    
    if (!inventoryResponse.IsSuccessStatusCode)
    {
        return Results.NotFound($"Product '{order.Product}' not found in inventory");
    }
    
    var product = await inventoryResponse.Content.ReadFromJsonAsync<ProductDto>();
    
    if (product.Stock < order.Quantity)
    {
        return Results.BadRequest($"Insufficient stock. Available: {product.Stock}, Requested: {order.Quantity}");
    }
    
    context.Orders.Add(order);
    await context.SaveChangesAsync();
    
    var orderCreatedEvent = new OrderCreatedEvent
    {
        OrderId = order.Id,
        Product = order.Product,
        Quantity = order.Quantity,
        CustomerName = order.Name,
        CreatedAt = DateTime.UtcNow
    };
    
    await sqsService.PublishMessageAsync("order-created", orderCreatedEvent);
    Console.WriteLine($"📤 Published OrderCreated event for Order #{order.Id}");
    
    return Results.Created($"/orders/{order.Id}", order);
}).RequireAuthorization();

app.Run();

public class ProductDto
{
    public int Id { get; set; }
    public string ProductName { get; set; }
    public int Stock { get; set; }
}