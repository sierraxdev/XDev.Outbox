using Azure.Messaging.ServiceBus;
using System.Text.Json;

namespace XDev.Outbox.ServiceBus
{
    public class ServiceBusMessageQueueOptions
    {
        public string QueueName { get; set; }
        public JsonSerializerOptions SerializerOptions { get; set; }
        public ServiceBusProcessorOptions ServiceBusOptions { get; set; }
    }
}
