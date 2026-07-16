using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

public class SystemTextJsonSerializerTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    // === Serialize Tests ===

    [Fact]
    public void Serialize_SimpleObject_ProducesCamelCaseJson()
    {
        var obj = new { FirstName = "John", LastName = "Doe", Age = 30 };
        var json = SerializeToString(obj);
        json.Should().Contain("\"firstName\":");
        json.Should().Contain("\"lastName\":");
        json.Should().Contain("\"age\":30");
    }

    [Fact]
    public void Serialize_NullProperties_AreOmitted()
    {
        var obj = new TestDoc { Name = "test", Description = null };
        var json = SerializeToString(obj);
        json.Should().Contain("\"name\":");
        json.Should().NotContain("\"description\":");
    }

    [Fact]
    public void Serialize_NullData_WritesNothing()
    {
        using var stream = new MemoryStream();
        _serializer.Serialize<TestDoc>(null!, stream);
        stream.Length.Should().Be(0);
    }

    [Fact]
    public void Serialize_WithIndentedFormatting_ProducesFormattedJson()
    {
        var obj = new { Name = "test" };
        using var stream = new MemoryStream();
        _serializer.Serialize(obj, stream, SerializationFormatting.Indented);
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\n"); // indented output has newlines
    }

    // === Deserialize Tests ===

    [Fact]
    public void Deserialize_CamelCaseJson_MapsToProperties()
    {
        var json = "{\"name\":\"test\",\"age\":25}";
        var result = DeserializeFromString<TestDoc>(json);
        result!.Name.Should().Be("test");
        result.Age.Should().Be(25);
    }

    [Fact]
    public void Deserialize_PascalCaseJson_StillMaps()
    {
        var json = "{\"Name\":\"test\",\"Age\":25}";
        var result = DeserializeFromString<TestDoc>(json);
        result!.Name.Should().Be("test");
        result.Age.Should().Be(25);
    }

    [Fact]
    public void Deserialize_NullStream_ReturnsDefault()
    {
        var result = _serializer.Deserialize<TestDoc>(Stream.Null);
        ((object?)result).Should().BeNull();
    }

    [Fact]
    public void Deserialize_EmptyStream_ReturnsDefault()
    {
        using var stream = new MemoryStream();
        var result = _serializer.Deserialize<TestDoc>(stream);
        ((object?)result).Should().BeNull();
    }

    [Fact]
    public void Deserialize_ValueType_NullStream_ReturnsDefault()
    {
        var result = _serializer.Deserialize(typeof(int), Stream.Null);
        result.Should().Be(0); // default(int)
    }

    // === Async Tests ===

    [Fact]
    public async Task SerializeAsync_ProducesSameResultAsSync()
    {
        var obj = new TestDoc { Name = "async", Age = 42 };

        using var syncStream = new MemoryStream();
        _serializer.Serialize(obj, syncStream);

        using var asyncStream = new MemoryStream();
        await _serializer.SerializeAsync(obj, asyncStream);

        syncStream.ToArray().Should().Equal(asyncStream.ToArray());
    }

    [Fact]
    public async Task DeserializeAsync_ProducesSameResultAsSync()
    {
        var json = "{\"name\":\"test\",\"age\":10}";

        var syncResult = DeserializeFromString<TestDoc>(json);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var asyncResult = await _serializer.DeserializeAsync<TestDoc>(stream);

        asyncResult!.Name.Should().Be(syncResult!.Name);
        asyncResult.Age.Should().Be(syncResult.Age);
    }

    // === Roundtrip Tests ===

    [Fact]
    public void Roundtrip_ComplexObject_PreservesData()
    {
        var original = new TestDoc { Name = "roundtrip", Age = 99, Description = "test desc" };
        using var stream = new MemoryStream();
        _serializer.Serialize(original, stream);
        stream.Position = 0;
        var deserialized = _serializer.Deserialize<TestDoc>(stream);
        deserialized!.Name.Should().Be("roundtrip");
        deserialized.Age.Should().Be(99);
        deserialized.Description.Should().Be("test desc");
    }

    // === Helpers ===

    private string SerializeToString<T>(T obj)
    {
        using var stream = new MemoryStream();
        _serializer.Serialize(obj, stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private T? DeserializeFromString<T>(string json)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return _serializer.Deserialize<T>(stream);
    }

    private class TestDoc
    {
        public string? Name { get; set; }
        public int Age { get; set; }
        public string? Description { get; set; }
    }
}
