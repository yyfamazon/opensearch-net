/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using FluentAssertions;
using OpenSearch.Client;
using Xunit;

namespace Tests.Serialization.Client;

public class SearchConverterTests
{
    [Fact]
    public void TotalHits_Deserializes_FromObjectFormat()
    {
        var json = """{"value":1000,"relation":"eq"}""";

        var totalHits = SerializerHelper.Deserialize<TotalHits>(json);

        totalHits.Should().NotBeNull();
        totalHits!.Value.Should().Be(1000);
        totalHits.Relation.Should().Be(TotalHitsRelation.EqualTo);
    }

    [Fact]
    public void TotalHits_Deserializes_FromNumberFormat()
    {
        var json = "42";

        var totalHits = SerializerHelper.Deserialize<TotalHits>(json);

        totalHits.Should().NotBeNull();
        totalHits!.Value.Should().Be(42);
        totalHits.Relation.Should().BeNull();
    }

    [Fact]
    public void TrackTotalHits_Bool_Serializes()
    {
        TrackTotalHits trackTotalHits = true;

        var json = SerializerHelper.Serialize(trackTotalHits);

        json.Should().Contain("true");
    }

    [Fact]
    public void TrackTotalHits_Number_Serializes()
    {
        TrackTotalHits trackTotalHits = 10000L;

        var json = SerializerHelper.Serialize(trackTotalHits);

        json.Should().Contain("10000");
        json.Should().NotContain("\"10000\"");
    }
}
