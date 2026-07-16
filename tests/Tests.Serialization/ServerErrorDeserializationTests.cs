/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System.IO;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

/// <summary>
/// Tests that ServerError deserialization works with the new SystemTextJsonSerializer.
/// Covers: ServerError.cs changes (Create/CreateAsync now use SystemTextJsonSerializer)
/// </summary>
public class ServerErrorDeserializationTests
{
    [Fact]
    public void ServerError_Create_DeserializesFromJson()
    {
        var json = "{\"error\":{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [test]\",\"index\":\"test\"},\"status\":404}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var error = ServerError.Create(stream);

        // Utf8Json (the default serializer) uses IL emit to populate internal setters
        error.Should().NotBeNull();
        error!.Status.Should().Be(404);
        error.Error.Should().NotBeNull();
        error.Error!.Type.Should().Be("index_not_found_exception");
        error.Error.Reason.Should().Be("no such index [test]");
    }

    [Fact]
    public async Task ServerError_CreateAsync_DeserializesFromJson()
    {
        var json = "{\"error\":{\"type\":\"resource_already_exists_exception\",\"reason\":\"index already exists\"},\"status\":400}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var error = await ServerError.CreateAsync(stream);

        // Utf8Json (the default serializer) uses IL emit to populate internal setters
        error.Should().NotBeNull();
        error!.Status.Should().Be(400);
        error.Error.Should().NotBeNull();
        error.Error!.Type.Should().Be("resource_already_exists_exception");
        error.Error.Reason.Should().Be("index already exists");
    }

    [Fact]
    public void ServerError_Create_NullStream_ReturnsNull()
    {
        var error = ServerError.Create(Stream.Null);
        error.Should().BeNull();
    }

    [Fact]
    public void ServerError_Create_EmptyStream_ReturnsNull()
    {
        using var stream = new MemoryStream();
        var error = ServerError.Create(stream);
        error.Should().BeNull();
    }
}

/// <summary>
/// Tests DynamicDictionary deserialization via SystemTextJsonSerializer
/// as used by ResponseBuilder for dynamic responses.
/// Covers: ResponseBuilder.cs change
/// </summary>
public class DynamicResponseDeserializationTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    [Fact]
    public void DynamicDictionary_DeserializesOpenSearchResponse()
    {
        var json = "{\"took\":5,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"hits\":[]}}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var result = _serializer.Deserialize<DynamicDictionary>(stream);

        ((object?)result).Should().NotBeNull();
        ((long)result!["took"]).Should().Be(5);
        ((bool)result["timed_out"]).Should().Be(false);
    }

    [Fact]
    public void DynamicDictionary_HandlesNestedObjects()
    {
        var json = "{\"_shards\":{\"total\":5,\"successful\":5}}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var result = _serializer.Deserialize<DynamicDictionary>(stream);

        ((object?)result).Should().NotBeNull();
        result!["_shards"].Should().NotBeNull();
    }

    [Fact]
    public void DynamicDictionary_HandlesArrayValues()
    {
        var json = "{\"hits\":[{\"_id\":\"1\"},{\"_id\":\"2\"}]}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var result = _serializer.Deserialize<DynamicDictionary>(stream);

        ((object?)result).Should().NotBeNull();
        result!["hits"].Should().NotBeNull();
    }
}
