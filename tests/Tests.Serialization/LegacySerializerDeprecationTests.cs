/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.IO;
using System.Reflection;
using System.Text;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

/// <summary>
/// Tests that the deprecated LowLevelRequestResponseSerializer still functions
/// but is properly marked as obsolete.
/// Covers: LowLevelRequestResponseSerializer.cs [Obsolete] attribute addition
/// </summary>
public class LegacySerializerDeprecationTests
{
    [Fact]
#pragma warning disable CS0618
    public void LowLevelRequestResponseSerializer_IsMarkedObsolete()
    {
        var attr = typeof(LowLevelRequestResponseSerializer)
            .GetCustomAttribute<ObsoleteAttribute>();

        attr.Should().NotBeNull();
        attr!.Message.Should().Contain("SystemTextJsonSerializer");
        attr.Message.Should().Contain("deprecated");
    }
#pragma warning restore CS0618

    [Fact]
    public void LowLevelRequestResponseSerializer_StillFunctions()
    {
        // Even though deprecated, it should still work for backward compatibility
#pragma warning disable CS0618
        var serializer = LowLevelRequestResponseSerializer.Instance;
#pragma warning restore CS0618

        serializer.Should().NotBeNull();

        var obj = new { Name = "test", Count = 42 };
        using var stream = new MemoryStream();
        serializer.Serialize(obj, stream);
        stream.Length.Should().BeGreaterThan(0);

        stream.Position = 0;
        var result = serializer.Deserialize<DynamicDictionary>(stream);
        ((object?)result).Should().NotBeNull();
    }

    [Fact]
    public void SystemTextJsonSerializer_IsNotMarkedObsolete()
    {
        var attr = typeof(SystemTextJsonSerializer)
            .GetCustomAttribute<ObsoleteAttribute>();

        attr.Should().BeNull();
    }

    [Fact]
    public void SystemTextJsonSerializer_HasStaticInstance()
    {
        SystemTextJsonSerializer.Instance.Should().NotBeNull();
        SystemTextJsonSerializer.Instance.Should().BeSameAs(SystemTextJsonSerializer.Instance);
    }

    [Fact]
    public void SystemTextJsonSerializer_ImplementsIOpenSearchSerializer()
    {
        var serializer = SystemTextJsonSerializer.Instance;
        serializer.Should().BeAssignableTo<IOpenSearchSerializer>();
    }
}
