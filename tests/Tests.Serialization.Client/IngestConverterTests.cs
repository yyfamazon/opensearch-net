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

public class IngestConverterTests
{
    [Fact]
    public void Processor_Serializes_WithTypeAsKey()
    {
        var processor = new RenameProcessor
        {
            Field = "old_field",
            TargetField = "new_field"
        };

        var pipeline = new Pipeline
        {
            Processors = new List<IProcessor> { processor }
        };

        var json = SerializerHelper.Serialize(pipeline);

        json.Should().Contain("\"rename\"");
        json.Should().Contain("\"field\"");
        json.Should().Contain("\"target_field\"");
    }

    [Fact]
    public void Pipeline_MultipleProcessors_Serialize()
    {
        var pipeline = new Pipeline
        {
            Description = "test pipeline",
            Processors = new List<IProcessor>
            {
                new RenameProcessor
                {
                    Field = "field1",
                    TargetField = "field2"
                },
                new RenameProcessor
                {
                    Field = "field3",
                    TargetField = "field4"
                }
            }
        };

        var json = SerializerHelper.Serialize(pipeline);

        json.Should().Contain("\"description\"");
        json.Should().Contain("test pipeline");
        json.Should().Contain("\"processors\"");
        json.Should().Contain("\"rename\"");
        json.Should().Contain("field1");
        json.Should().Contain("field3");
    }
}
