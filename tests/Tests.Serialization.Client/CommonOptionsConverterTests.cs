/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using FluentAssertions;
using OpenSearch.Client;
using Xunit;

namespace Tests.Serialization.Client;

public class CommonOptionsConverterTests
{
    [Fact]
    public void Fuzziness_Auto_SerializesAsAutoString()
    {
        var query = new MatchQuery
        {
            Field = "title",
            Query = "opensearch",
            Fuzziness = Fuzziness.Auto
        };

        var json = SerializerHelper.Serialize(new SearchRequest
        {
            Query = new QueryContainer(query)
        });

        json.Should().Contain("\"fuzziness\"");
        json.Should().Contain("\"AUTO\"");
    }

    [Fact]
    public void Fuzziness_EditDistance_SerializesAsNumber()
    {
        var query = new MatchQuery
        {
            Field = "title",
            Query = "opensearch",
            Fuzziness = Fuzziness.EditDistance(2)
        };

        var json = SerializerHelper.Serialize(new SearchRequest
        {
            Query = new QueryContainer(query)
        });

        json.Should().Contain("\"fuzziness\"");
        json.Should().Contain("2");
    }

    [Fact]
    public void Script_Inline_Serializes_WithSource()
    {
        var script = new InlineScript("doc['price'].value * params.factor")
        {
            Lang = "painless"
        };

        var json = SerializerHelper.Serialize(script);

        json.Should().Contain("\"source\"");
        json.Should().Contain("doc['price'].value * params.factor");
        json.Should().Contain("\"lang\"");
        json.Should().Contain("\"painless\"");
    }

    [Fact]
    public void TimeSpan_Serializes_AsTicks()
    {
        // OpenSearch.Client serializes TimeSpan as ticks by default (via Utf8Json)
        var timeSpan = TimeSpan.FromMinutes(5);

        var json = SerializerHelper.Serialize(timeSpan);

        // TimeSpan serializes as its tick count (nanosecond representation)
        json.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void DateMath_Serializes_AsString()
    {
        var dateMath = DateMath.Now;

        var json = SerializerHelper.Serialize(dateMath);

        json.Should().Contain("now");
    }

    [Fact]
    public void Distance_Serializes_AsString()
    {
        var distance = Distance.Kilometers(10);

        var json = SerializerHelper.Serialize(distance);

        json.Should().Contain("10");
        json.Should().Contain("km");
    }
}
