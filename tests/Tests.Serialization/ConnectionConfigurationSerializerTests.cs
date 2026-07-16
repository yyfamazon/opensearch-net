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
/// Tests that ConnectionConfiguration now defaults to SystemTextJsonSerializer
/// instead of the deprecated LowLevelRequestResponseSerializer (Utf8Json).
/// Covers: ConnectionConfiguration.cs change
/// </summary>
public class ConnectionConfigurationSerializerTests
{
    [Fact]
    public void DefaultSerializer_IsSystemTextJson()
    {
        var config = new ConnectionConfiguration();
        var values = (IConnectionConfigurationValues)config;

        // The serializer is wrapped in DiagnosticsSerializerProxy, but it wraps SystemTextJsonSerializer
        values.RequestResponseSerializer.Should().NotBeNull();
        // Verify it produces camelCase JSON (SystemTextJsonSerializer behavior)
        using var stream = new MemoryStream();
        values.RequestResponseSerializer.Serialize(new { TestProperty = "value" }, stream);
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\"testProperty\"");
    }

    [Fact]
    public void DefaultSerializer_OmitsNulls()
    {
        var config = new ConnectionConfiguration();
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
