namespace XDev.Outbox.EFCore
{
    public class OutboxDbContextConfiguration
    {
        public string TableName { get; set; } = "OutboxMessages";
    }
}
