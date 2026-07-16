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

public class AnalysisConverterTests
{
    [Fact]
    public void StopWords_PredefinedList_SerializesAsString()
    {
        StopWords stopWords = "_english_";

        var json = SerializerHelper.Serialize(stopWords);

        json.Should().Contain("_english_");
        // Should be a plain string, not an array
        json.Should().NotContain("[");
    }

    [Fact]
    public void StopWords_CustomArray_SerializesAsArray()
    {
        StopWords stopWords = new List<string> { "the", "a", "an" };

        var json = SerializerHelper.Serialize(stopWords);

        json.Should().Contain("[");
        json.Should().Contain("\"the\"");
        json.Should().Contain("\"a\"");
        json.Should().Contain("\"an\"");
    }
}
