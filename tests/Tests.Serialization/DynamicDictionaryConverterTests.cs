using System.IO;
using System.Text;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

public class DynamicDictionaryConverterTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    [Fact]
    public void Deserialize_JsonObject_CreatesDynamicDictionary()
    {
        var json = "{\"name\":\"test\",\"count\":42,\"active\":true}";
        var result = Deserialize<DynamicDictionary>(json);

        ((object?)result).Should().NotBeNull();
        ((string)result!["name"]).Should().Be("test");
        ((long)result["count"]).Should().Be(42);
        ((bool)result["active"]).Should().Be(true);
    }

    [Fact]
    public void Deserialize_JsonArray_CreatesDictionaryWithNumericKeys()
    {
        var json = "[\"first\",\"second\",\"third\"]";
        var result = Deserialize<DynamicDictionary>(json);

        ((object?)result).Should().NotBeNull();
        ((string)result!["0"]).Should().Be("first");
        ((string)result["1"]).Should().Be("second");
        ((string)result["2"]).Should().Be("third");
    }

    [Fact]
    public void Deserialize_NestedObject_HandlesRecursion()
    {
        var json = "{\"outer\":{\"inner\":\"value\"}}";
        var result = Deserialize<DynamicDictionary>(json);

        ((object?)result).Should().NotBeNull();
        result!["outer"].Should().NotBeNull();
    }

    [Fact]
    public void Deserialize_NullValue_ReturnsNull()
    {
        var json = "null";
        var result = Deserialize<DynamicDictionary>(json);
        ((object?)result).Should().BeNull();
    }

    [Fact]
    public void Serialize_DynamicDictionary_ProducesJsonObject()
    {
        var dict = new DynamicDictionary();
        dict["name"] = new DynamicValue("test");
        dict["count"] = new DynamicValue(42);

        var json = Serialize(dict);
        json.Should().Contain("\"name\":\"test\"");
        json.Should().Contain("\"count\":42");
    }

    [Fact]
    public void Serialize_EmptyDictionary_ProducesEmptyObject()
    {
        var dict = new DynamicDictionary();
        var json = Serialize(dict);
        json.Should().Be("{}");
    }

    [Fact]
    public void Serialize_NullDictionary_WritesNothing()
    {
        DynamicDictionary? dict = null;
        using var stream = new MemoryStream();
        _serializer.Serialize(dict, stream);
        stream.Length.Should().Be(0);
    }

    // Helpers
    private T? Deserialize<T>(string json)
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
