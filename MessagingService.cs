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
        private IHubContext<NotifyHub> _hub;
        private SubscriptionName? _subscriptionName;
        private SubscriberClient? _subscriber;
        private Task? _processorTask;

        public MessagingService(ILogger<MessagingService> logger, IConfiguration config, IHubContext<NotifyHub> hub)
        {
            _topicId = config["TopicId"];
            _projectId = config["ProjectId"];

            if (_topicId == null || _projectId == null)
                throw new ArgumentNullException(
                    "You must configure values for PubSub `TopicId`, `ProjectId`");

            _subscriptionId = $"{_topicId}_{Guid.NewGuid().ToString()}";
            _log = logger;
            _hub = hub;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            CreateSubscription();
            _subscriber = await SubscriberClient.CreateAsync(_subscriptionName);
            _processorTask = _subscriber.StartAsync(ProcessMessageAsync);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_subscriber != null)
            {
                await _subscriber.StopAsync(CancellationToken.None);
            }
            
            DeleteSubscription();
        }

        public async Task Publish(string message)
        {
            TopicName topicName = TopicName.FromProjectTopic(_projectId, _topicId);
            PublisherClient publisher = await PublisherClient.CreateAsync(topicName);

            try
            {
                string result = await publisher.PublishAsync(message);
                _log.LogDebug($"Published message: {message}");
            }
            catch (Exception exception)
            {
                _log.LogError($"An error ocurred when publishing message {message}: " +
                    $"{exception.Message}");
            }            
        }

        private void DeleteSubscription()
        {
            SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
            subscriber.DeleteSubscription(_subscriptionName);      
            _log.LogInformation($"Deleted subscription: {_subscriptionId}");      
        }

        private void CreateSubscription()
        {
            SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
            TopicName topicName = TopicName.FromProjectTopic(_projectId, _topicId);

            _subscriptionName = SubscriptionName.FromProjectSubscription(
                _projectId, _subscriptionId);

            try
            {
                subscriber.CreateSubscription(
                    _subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
                
                _log.LogInformation($"Created subscription: {_subscriptionId}");
                
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