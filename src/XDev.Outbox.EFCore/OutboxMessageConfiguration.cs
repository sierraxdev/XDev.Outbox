using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.Extensions.Options;
using System.Dynamic;
using System.Text.Json;
using XDev.Outbox;
using XDev.Outbox.EFCore;

namespace Microsoft.Extensions.DependencyInjection
{
    public class OutboxMessageConfiguration<TKey> : IEntityTypeConfiguration<OutboxMessage<TKey>> where TKey : struct
    {
        private readonly OutboxDbContextConfiguration _configuration;
        public OutboxMessageConfiguration(IOptions<OutboxDbContextConfiguration> options)
        {
            _configuration = options.Value;
        }

        public void Configure(EntityTypeBuilder<OutboxMessage<TKey>> builder)
        {
            builder.ToTable(_configuration?.TableName ?? nameof(IOutboxDbContext<TKey>.OutboxMessages));

            builder.HasKey(e => e.Id);

            builder.Property(e => e.Type).HasMaxLength(200);

            builder.Property(e => e.Payload)
                .HasConversion(
                    v => JsonSerializer.Serialize(v, OutboxConfiguration.SerializerOptions),
                    v => JsonSerializer.Deserialize(v, typeof(ExpandoObject), OutboxConfiguration.SerializerOptions)
                );

            //builder.OwnsOne(e => e.Payload, b =>
            //{
            //    b.ToJson();
            //});
        }
    }
}
