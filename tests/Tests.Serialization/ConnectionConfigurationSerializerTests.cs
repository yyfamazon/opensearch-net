/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.IO;
using System.Text;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

/// <summary>
/// Tests that ConnectionConfiguration defaults to Utf8Json (LowLevelRequestResponseSerializer)
/// for backward compatibility, and that STJ requires explicit opt-in.
/// Covers: ConnectionConfiguration.cs change
/// </summary>
public class ConnectionConfigurationSerializerTests
{
    [Fact]
    public void DefaultSerializer_IsUtf8Json()
    {
        var config = new ConnectionConfiguration();
        var values = (IConnectionConfigurationValues)config;

        // The default serializer is still Utf8Json (wrapped in DiagnosticsSerializerProxy)
        values.RequestResponseSerializer.Should().NotBeNull();
        // Utf8Json produces PascalCase by default for anonymous types
        using var stream = new MemoryStream();
        values.RequestResponseSerializer.Serialize(new { TestProperty = "value" }, stream);
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\"TestProperty\"");
    }

    [Fact]
    public void SystemTextJsonSerializer_ProducesCamelCase()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
        var config = new ConnectionConfiguration(pool, null, SystemTextJsonSerializer.Instance);
        var values = (IConnectionConfigurationValues)config;

        using var stream = new MemoryStream();
        values.RequestResponseSerializer.Serialize(new { TestProperty = "value" }, stream);
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\"testProperty\"");
    }

    [Fact]
    public void SystemTextJsonSerializer_OmitsNulls()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
        var config = new ConnectionConfiguration(pool, null, SystemTextJsonSerializer.Instance);
        var values = (IConnectionConfigurationValues)config;

        using var stream = new MemoryStream();
        values.RequestResponseSerializer.Serialize(new NullableDoc { Name = "test", Value = null }, stream);
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\"name\"");
        json.Should().NotContain("\"value\"");
    }

    [Fact]
    public void CustomSerializer_CanBeProvided()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
#pragma warning disable CS0618
        var config = new ConnectionConfiguration(pool, null, new LowLevelRequestResponseSerializer());
#pragma warning restore CS0618
        var values = (IConnectionConfigurationValues)config;

        // Should still work with the legacy serializer
        values.RequestResponseSerializer.Should().NotBeNull();
    }

    private class NullableDoc
    {
        public string? Name { get; set; }
        public string? Value { get; set; }
    }
}
