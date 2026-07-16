/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

/// <summary>
/// Tests the SystemTextJsonSerializer.Instance static singleton pattern
/// and verifies it matches the behavior expected by the OpenSearch.Net transport layer.
/// </summary>
public class SystemTextJsonSerializerIntegrationTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    // === OpenSearch-specific JSON patterns ===

    [Fact]
    public void Handles_NumbersAsStrings_FromOpenSearch()
    {
        // OpenSearch sometimes returns numbers as strings
        var json = "{\"count\":\"42\",\"name\":\"test\"}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var result = _serializer.Deserialize<DocWithNullableInt>(stream);
        result!.Count.Should().Be(42);
        result.Name.Should().Be("test");
    }

    [Fact]
    public void Handles_MixedCasing_FromOpenSearch()
    {
        // OpenSearch may return snake_case or camelCase
        var json = "{\"node_name\":\"node-1\",\"nodeName\":\"node-2\"}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        // Case-insensitive mapping should work
        var result = _serializer.Deserialize<DynamicDictionary>(stream);
        ((object?)result).Should().NotBeNull();
    }

    [Fact]
    public void Serialize_Dictionary_PreservesKeys()
    {
        var dict = new Dictionary<string, object>
        {
            ["_index"] = "test-index",
            ["_id"] = "doc-1",
            ["_score"] = 1.5
        };

        var json = SerializeToString(dict);
        json.Should().Contain("\"_index\":\"test-index\"");
        json.Should().Contain("\"_id\":\"doc-1\"");
        json.Should().Contain("\"_score\":1.5");
    }

    [Fact]
    public void Serialize_NestedObjects_ProducesValidJson()
    {
        var obj = new
        {
            Query = new
            {
                Match = new { Title = "opensearch" }
            },
            Size = 10
        };

        var json = SerializeToString(obj);
        json.Should().Contain("\"query\":{\"match\":{\"title\":\"opensearch\"}}");
        json.Should().Contain("\"size\":10");
    }

    [Fact]
    public void Serialize_Arrays_ProducesValidJson()
    {
        var obj = new { Fields = new[] { "field1", "field2", "field3" } };
        var json = SerializeToString(obj);
        json.Should().Contain("\"fields\":[\"field1\",\"field2\",\"field3\"]");
    }

    [Fact]
    public void Serialize_BooleanValues_AreLowercase()
    {
        var obj = new { Enabled = true, Disabled = false };
        var json = SerializeToString(obj);
        json.Should().Contain("\"enabled\":true");
        json.Should().Contain("\"disabled\":false");
    }

    [Fact]
    public void Deserialize_LargeDocument_DoesNotThrow()
    {
        var largeArray = new List<string>();
        for (int i = 0; i < 10000; i++)
            largeArray.Add($"item-{i}");

        var json = System.Text.Json.JsonSerializer.Serialize(new { items = largeArray });
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var action = () => _serializer.Deserialize<DynamicDictionary>(stream);
        action.Should().NotThrow();
    }

    // === Type handling ===

    [Fact]
    public void Deserialize_ByType_ReturnsCorrectType()
    {
        var json = "{\"name\":\"test\"}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var result = _serializer.Deserialize(typeof(DocWithNullableInt), stream);
        result.Should().BeOfType<DocWithNullableInt>();
        ((DocWithNullableInt)result!).Name.Should().Be("test");
    }

    [Fact]
    public async Task DeserializeAsync_ByType_ReturnsCorrectType()
    {
        var json = "{\"name\":\"test\"}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var result = await _serializer.DeserializeAsync(typeof(DocWithNullableInt), stream);
        result.Should().BeOfType<DocWithNullableInt>();
    }

    // === Edge cases ===

    [Fact]
    public void Deserialize_EmptyObject_ReturnsInstance()
    {
        var json = "{}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var result = _serializer.Deserialize<DocWithNullableInt>(stream);
        result.Should().NotBeNull();
        result!.Name.Should().BeNull();
        result.Count.Should().BeNull();
    }

    [Fact]
    public void Deserialize_ExtraProperties_AreIgnored()
    {
        var json = "{\"name\":\"test\",\"unknownField\":\"ignored\",\"anotherUnknown\":123}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var result = _serializer.Deserialize<DocWithNullableInt>(stream);
        result!.Name.Should().Be("test");
    }

    [Fact]
    public void Roundtrip_PreservesAllDataTypes()
    {
        var original = new AllTypesDoc
        {
            StringVal = "hello",
            IntVal = 42,
            LongVal = 9876543210L,
            DoubleVal = 3.14159,
            BoolVal = true,
            DateVal = new DateTime(2024, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            ArrayVal = new[] { "a", "b", "c" }
        };

        using var stream = new MemoryStream();
        _serializer.Serialize(original, stream);
        stream.Position = 0;
        var result = _serializer.Deserialize<AllTypesDoc>(stream);

        result!.StringVal.Should().Be("hello");
        result.IntVal.Should().Be(42);
        result.LongVal.Should().Be(9876543210L);
        result.DoubleVal.Should().BeApproximately(3.14159, 0.00001);
        result.BoolVal.Should().BeTrue();
        result.DateVal.Should().Be(new DateTime(2024, 7, 2, 12, 0, 0, DateTimeKind.Utc));
        result.ArrayVal.Should().BeEquivalentTo(new[] { "a", "b", "c" });
    }

    // === Helpers ===

    private string SerializeToString<T>(T obj)
    {
        using var stream = new MemoryStream();
        _serializer.Serialize(obj, stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private class DocWithNullableInt
    {
        public string? Name { get; set; }
        public int? Count { get; set; }
    }

    private class AllTypesDoc
    {
        public string? StringVal { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public double DoubleVal { get; set; }
        public bool BoolVal { get; set; }
        public DateTime DateVal { get; set; }
        public string[]? ArrayVal { get; set; }
    }
}
