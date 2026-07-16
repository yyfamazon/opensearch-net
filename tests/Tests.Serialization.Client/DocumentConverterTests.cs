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

public class DocumentConverterTests
{
    [Fact]
    public void SourceFilter_Disabled_SerializesAsFalse()
    {
        var request = new SearchRequest
        {
            Source = false
        };

        var json = SerializerHelper.Serialize(request);

        json.Should().Contain("\"_source\"");
        json.Should().Contain("false");
    }

    [Fact]
    public void SourceFilter_WithIncludes_SerializesAsObject()
    {
        var request = new SearchRequest
        {
            Source = new SourceFilter
            {
                Includes = new[] { "name", "age" }
            }
        };

        var json = SerializerHelper.Serialize(request);

        json.Should().Contain("\"_source\"");
        json.Should().Contain("\"includes\"");
        json.Should().Contain("name");
        json.Should().Contain("age");
    }

    [Fact]
    public void Slices_Auto_SerializesAsString()
    {
        Slices slices = "auto";

        var json = SerializerHelper.Serialize(slices);

        json.Should().Contain("auto");
    }

    [Fact]
    public void Slices_Number_SerializesAsNumber()
    {
        Slices slices = 5L;

        var json = SerializerHelper.Serialize(slices);

        json.Should().Contain("5");
        json.Should().NotContain("\"5\"");
    }
}
