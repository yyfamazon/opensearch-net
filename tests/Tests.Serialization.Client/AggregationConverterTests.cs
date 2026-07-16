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

public class AggregationConverterTests
{
    [Fact]
    public void TermsAggregation_Serializes_WithField()
    {
        var agg = new TermsAggregation("status_terms")
        {
            Field = "status"
        };

        var json = SerializerHelper.Serialize(agg);

        json.Should().Contain("\"field\"");
        json.Should().Contain("\"status\"");
    }

    [Fact]
    public void AggregationDictionary_Serializes_NamedAggregations()
    {
        var request = new SearchRequest
        {
            Aggregations = new TermsAggregation("by_status")
            {
                Field = "status"
            }
        };

        var json = SerializerHelper.Serialize(request);

        json.Should().Contain("\"aggs\"");
        json.Should().Contain("\"by_status\"");
        json.Should().Contain("\"terms\"");
        json.Should().Contain("\"field\"");
    }

    [Fact]
    public void NestedSubAggregations_Serialize()
    {
        var request = new SearchRequest
        {
            Aggregations = new TermsAggregation("by_status")
            {
                Field = "status",
                Aggregations = new AverageAggregation("avg_score", "score")
            }
        };

        var json = SerializerHelper.Serialize(request);

        json.Should().Contain("\"by_status\"");
        json.Should().Contain("\"terms\"");
        json.Should().Contain("\"aggs\"");
        json.Should().Contain("\"avg_score\"");
        json.Should().Contain("\"avg\"");
    }
}
