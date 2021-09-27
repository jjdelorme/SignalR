using Microsoft.AspNetCore.SignalR;
using signalR;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();

app.UseEndpoints(endpoints =>
    endpoints.MapHub<ChatHub>("/chathub")
);

void SendMessages(WebApplication app)
{
    var hub = app.Services.GetRequiredService<IHubContext<ChatHub>>();
    var clients = hub.Clients.All;
    var timer = new Timer(new TimerCallback( 
        async (_) => await clients.SendAsync("ReceiveMessage", "Welcome")),
        null, 1000, 2000
    );
}

SendMessages(app);
app.Run();

