using System.IO;
using System.Text;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

public class NullableStringIntConverterTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    [Fact]
    public void Deserialize_NumberToken_ReturnsInt()
    {
        var result = Deserialize<IntWrapper>("{\"value\":42}");
        result.Value.Should().Be(42);
    }

    [Fact]
    public void Deserialize_StringNumber_ReturnsInt()
    {
        var result = Deserialize<IntWrapper>("{\"value\":\"42\"}");
        result.Value.Should().Be(42);
    }

    [Fact]
    public void Deserialize_NullToken_ReturnsNull()
    {
        var result = Deserialize<IntWrapper>("{\"value\":null}");
        result.Value.Should().BeNull();
    }

    [Fact]
    public void Deserialize_EmptyString_ReturnsNull()
    {
        var result = Deserialize<IntWrapper>("{\"value\":\"\"}");
        result.Value.Should().BeNull();
    }

    [Fact]
    public void Serialize_IntValue_WritesNumber()
    {
        var json = Serialize(new IntWrapper { Value = 100 });
        json.Should().Contain("\"value\":100");
    }

    [Fact]
    public void Serialize_NullValue_OmitsProperty()
    {
        var json = Serialize(new IntWrapper { Value = null });
        // With WhenWritingNull, null properties are omitted
        json.Should().NotContain("\"value\"");
    }

    private class IntWrapper
    {
        public int? Value { get; set; }
    }

    private T Deserialize<T>(string json)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return _serializer.Deserialize<T>(stream);
    }

    private string Serialize<T>(T obj)
    {
        using var stream = new MemoryStream();
        _serializer.Serialize(obj, stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
