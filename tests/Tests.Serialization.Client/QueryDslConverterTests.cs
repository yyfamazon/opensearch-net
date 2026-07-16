/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System.Collections.Generic;
using FluentAssertions;
using OpenSearch.Client;
using Xunit;

namespace Tests.Serialization.Client;

public class QueryDslConverterTests
{
    [Fact]
    public void BoolQuery_Serializes_WithMustArray()
    {
        var query = new BoolQuery
        {
            Must = new List<QueryContainer>
            {
                new QueryContainer(new TermQuery { Field = "status", Value = "active" })
            }
        };

        var json = SerializerHelper.Serialize(query);

        json.Should().Contain("\"must\"");
        json.Should().Contain("\"status\"");
    }

    [Fact]
    public void BoolQuery_Serializes_WithShouldAndFilter()
    {
        var query = new BoolQuery
        {
            Should = new List<QueryContainer>
            {
                new QueryContainer(new MatchQuery { Field = "title", Query = "hello" })
            },
            Filter = new List<QueryContainer>
            {
                new QueryContainer(new TermQuery { Field = "status", Value = "published" })
            }
        };

        var json = SerializerHelper.Serialize(query);

        json.Should().Contain("\"should\"");
        json.Should().Contain("\"filter\"");
    }

    [Fact]
    public void MatchQuery_Serializes_WithFieldNameAsKey()
    {
        var query = new MatchQuery { Field = "title", Query = "opensearch" };

        var json = SerializerHelper.Serialize(new SearchRequest
        {
            Query = new QueryContainer(query)
        });

        json.Should().Contain("\"match\"");
        json.Should().Contain("\"title\"");
        json.Should().Contain("opensearch");
    }

    [Fact]
    public void TermQuery_Serializes_WithFieldNameAsKey()
    {
        var query = new TermQuery { Field = "status", Value = "active" };

        var json = SerializerHelper.Serialize(new SearchRequest
        {
            Query = new QueryContainer(query)
        });

        json.Should().Contain("\"term\"");
        json.Should().Contain("\"status\"");
        json.Should().Contain("active");
    }

    [Fact]
    public void RangeQuery_Serializes_Numeric()
    {
        var query = new NumericRangeQuery
        {
            Field = "age",
            GreaterThanOrEqualTo = 18,
            LessThan = 65
        };

        var json = SerializerHelper.Serialize(new SearchRequest
        {
            Query = new QueryContainer(query)
        });

        json.Should().Contain("\"range\"");
        json.Should().Contain("\"age\"");
        json.Should().Contain("\"gte\"");
        json.Should().Contain("\"lt\"");
    }

    [Fact]
    public void QueryContainer_With_BoolQuery_HasDiscriminator()
    {
        var container = new QueryContainer(new BoolQuery
        {
            Must = new List<QueryContainer>
            {
                new QueryContainer(new MatchAllQuery())
            }
        });

        var json = SerializerHelper.Serialize(container);

        json.Should().Contain("\"bool\"");
        json.Should().Contain("\"must\"");
    }

    [Fact]
    public void NullQueryCollection_IsHandled()
    {
        var query = new BoolQuery
        {
            Must = null,
            Should = null
        };

        var json = SerializerHelper.Serialize(query);

        json.Should().NotContain("\"must\"");
        json.Should().NotContain("\"should\"");
    }
}
