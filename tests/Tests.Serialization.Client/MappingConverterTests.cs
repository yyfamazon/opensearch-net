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

public class MappingConverterTests
{
    [Fact]
    public void TextProperty_HasTypeText()
    {
        var property = new TextProperty();

        var json = SerializerHelper.Serialize(property);

        json.Should().Contain("\"type\"");
        json.Should().Contain("\"text\"");
    }

    [Fact]
    public void KeywordProperty_HasTypeKeyword()
    {
        var property = new KeywordProperty();

        var json = SerializerHelper.Serialize(property);

        json.Should().Contain("\"type\"");
        json.Should().Contain("\"keyword\"");
    }

    [Fact]
    public void Properties_SerializesAsFieldDictionary()
    {
        var properties = new Properties
        {
            { "name", new TextProperty() },
            { "status", new KeywordProperty() }
        };

        var json = SerializerHelper.Serialize(properties);

        json.Should().Contain("\"name\"");
        json.Should().Contain("\"status\"");
        json.Should().Contain("\"text\"");
        json.Should().Contain("\"keyword\"");
    }

    [Fact]
    public void JoinField_Parent_SerializesAsString()
    {
        var joinField = JoinField.Root("parent_type");

        var json = SerializerHelper.Serialize(joinField);

        json.Should().Contain("parent_type");
        // Parent join field serializes as a simple string
        json.Should().NotContain("{");
    }

    [Fact]
    public void JoinField_Child_SerializesAsObject()
    {
        var joinField = JoinField.Link("child_type", "parent_id_123");

        var json = SerializerHelper.Serialize(joinField);

        json.Should().Contain("child_type");
        json.Should().Contain("parent_id_123");
        // Child join field serializes as object with name and parent
        json.Should().Contain("\"name\"");
        json.Should().Contain("\"parent\"");
    }
}
