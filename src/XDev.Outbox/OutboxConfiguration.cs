using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace XDev.Outbox
{
    public class OutboxConfiguration
    {
        public int? MaxAttemps { get; set; }
        public TimeSpan MessageLockExpirationTimeSpan { get; set; } = TimeSpan.FromMinutes(5);

        private static object _serializerOptionsLock = new object();
        private static JsonSerializerOptions _serializerOptions;
        public static JsonSerializerOptions SerializerOptions
        {
            get
            {
                lock (_serializerOptionsLock)
                {
                    if (_serializerOptions == null)
                    {
                        _serializerOptions = new JsonSerializerOptions()
                        {
                            ReferenceHandler = ReferenceHandler.IgnoreCycles,
                            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                        };
                        _serializerOptions.Converters.Add(new JsonStringEnumConverter());
                    }
                }
                return _serializerOptions;
            }
            set
            {
                _serializerOptions = value;
            }
        }
    }
}
