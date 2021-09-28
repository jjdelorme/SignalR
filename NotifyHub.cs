using Microsoft.AspNetCore.SignalR;

namespace signalR
{
    public class NotifyHub : Hub
    {
        public async Task SendMessage(string user, string message)
        {
            await Clients.All.SendAsync("ReceiveMessage", user, message);
        }
    }
}