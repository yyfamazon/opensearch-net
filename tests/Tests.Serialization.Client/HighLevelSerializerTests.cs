/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using FluentAssertions;
using OpenSearch.Client;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization.Client;

public class HighLevelSerializerTests
{
    private class MyPoco
    {
        public string FirstName { get; set; } = null!;
        public int Age { get; set; }
        public string? NullableField { get; set; }
    }

    [Fact]
    public void ConnectionSettings_RequestResponseSerializer_IsNotNull()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
        var settings = new ConnectionSettings(pool);
        var serializer = ((IConnectionSettingsValues)settings).RequestResponseSerializer;

        serializer.Should().NotBeNull();
    }

    [Fact]
    public void ConnectionSettings_SourceSerializer_IsNotNull()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
        var settings = new ConnectionSettings(pool);
        var serializer = ((IConnectionSettingsValues)settings).SourceSerializer;

        serializer.Should().NotBeNull();
    }

    [Fact]
    public void Serialize_Poco_ProducesCamelCase()
    {
        var poco = new MyPoco { FirstName = "John", Age = 30 };

        var json = SerializerHelper.Serialize(poco);

        json.Should().Contain("\"firstName\"");
        json.Should().Contain("\"age\"");
    }

    [Fact]
    public void Deserialize_CamelCase_MapsToPoco()
    {
        var json = """{"firstName":"Jane","age":25}""";

        var poco = SerializerHelper.Deserialize<MyPoco>(json);

        poco.Should().NotBeNull();
        poco!.FirstName.Should().Be("Jane");
        poco.Age.Should().Be(25);
    }

    [Fact]
    public void Serialize_NullProperties_AreOmitted()
    {
        var poco = new MyPoco { FirstName = "John", Age = 30, NullableField = null };

        var json = SerializerHelper.Serialize(poco);

        json.Should().NotContain("\"nullableField\"");
    }
}
