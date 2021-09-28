using Text = System.Text;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.SignalR;

namespace signalR
{
    public class MessagingService : IHostedService
    {
        private ILogger<MessagingService> _log;
        private string _projectId;
        private string _topicId;
        private string _subscriptionId;
        private IHubContext<ChatHub> _hub;
        private SubscriptionName? _subscriptionName;

        public MessagingService(ILogger<MessagingService> logger, IConfiguration config, IHubContext<ChatHub> hub)
        {
            _log = logger;
            _subscriptionId = Guid.NewGuid().ToString();
            _topicId = config["TopicId"];
            _projectId = config["ProjectId"];
            _hub = hub;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            CreateSubscription();

            SubscriberClient subscriber = 
                await SubscriberClient.CreateAsync(_subscriptionName);

            await subscriber.StartAsync(ProcessMessageAsync);   
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            SubscriberClient subscriber = 
                await SubscriberClient.CreateAsync(_subscriptionName);
            
            await subscriber.StopAsync(CancellationToken.None);
            
            DeleteSubscription();
        }

        private void DeleteSubscription()
        {
            SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
            subscriber.DeleteSubscription(_subscriptionName);            
        }

        private void CreateSubscription()
        {
            SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
            TopicName topicName = TopicName.FromProjectTopic(_projectId, _topicId);

            _subscriptionName = SubscriptionName.FromProjectSubscription(
                _projectId, _subscriptionId);
            Subscription? subscription = null;

            try
            {
                subscription = subscriber.CreateSubscription(
                    _subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
            }
            catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
            {
                // Already exists.  That's fine.
            }
        }

        private async Task<SubscriberClient.Reply> ProcessMessageAsync(
            PubsubMessage message, 
            CancellationToken cancel)
        {
            SubscriberClient.Reply reply = SubscriberClient.Reply.Nack;

            if (cancel.IsCancellationRequested)
            {
                return reply;
            }

            try
            {               
                // Process the message.
                string json = Text.Encoding.UTF8.GetString(message.Data.ToArray());
                _log.LogInformation($"Received message id:{message.MessageId}, Body:{json}");

                await _hub.Clients.All.SendAsync("ReceiveMessage", json);

                reply = SubscriberClient.Reply.Ack;
            }
            catch (Exception e)
            {
                _log.LogError($"ERROR: Attempt for message {message.MessageId}.", e);
            }
            
            _log.LogInformation($"Message handler completed with {reply} for {message.MessageId}.");
            
            return reply;
        }
   }
}