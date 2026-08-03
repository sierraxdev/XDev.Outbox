using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore;

namespace Example
{
    public class ApplicationDbContext : OutboxDbContext<int>
    {
        public ApplicationDbContext(
            DbContextOptions<ApplicationDbContext> options) : base(options)
        {
        }

        public DbSet<User> Users { get; set; }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            builder.Entity<User>().Property(e => e.Email).HasMaxLength(200);
            base.OnModelCreating(builder);
        }
    }
}
