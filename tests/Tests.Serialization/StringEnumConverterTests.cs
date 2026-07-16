using System.IO;
using System.Runtime.Serialization;
using System.Text;
using FluentAssertions;
using OpenSearch.Net;
using Xunit;

namespace Tests.Serialization;

public class StringEnumConverterTests
{
    private readonly SystemTextJsonSerializer _serializer = SystemTextJsonSerializer.Instance;

    [StringEnum]
    public enum TestStatus
    {
        Active,
        Inactive,
        [EnumMember(Value = "pending_review")]
        PendingReview
    }

    public enum NumericStatus
    {
        Open = 0,
        Closed = 1
    }

    private class EnumWrapper
    {
        public TestStatus Status { get; set; }
        public TestStatus? NullableStatus { get; set; }
    }

    private class NumericEnumWrapper
    {
        public NumericStatus Status { get; set; }
    }

    [Fact]
    public void Serialize_StringEnum_WritesAsString()
    {
        var obj = new EnumWrapper { Status = TestStatus.Active };
        var json = Serialize(obj);
        json.Should().Contain("\"status\":\"active\"");
    }

    [Fact]
    public void Serialize_StringEnum_WithEnumMember_UsesCustomValue()
    {
        var obj = new EnumWrapper { Status = TestStatus.PendingReview };
        var json = Serialize(obj);
        json.Should().Contain("\"status\":\"pending_review\"");
    }

    [Fact]
    public void Deserialize_StringEnum_FromString()
    {
        var result = Deserialize<EnumWrapper>("{\"status\":\"active\"}");
        result.Status.Should().Be(TestStatus.Active);
    }

    [Fact]
    public void Deserialize_StringEnum_FromEnumMemberValue()
    {
        var result = Deserialize<EnumWrapper>("{\"status\":\"pending_review\"}");
        result.Status.Should().Be(TestStatus.PendingReview);
    }

    [Fact]
    public void Deserialize_StringEnum_CaseInsensitive()
    {
        var result = Deserialize<EnumWrapper>("{\"status\":\"ACTIVE\"}");
        result.Status.Should().Be(TestStatus.Active);
    }

    [Fact]
    public void Serialize_NullableStringEnum_Null_IsOmitted()
    {
        var obj = new EnumWrapper { Status = TestStatus.Active, NullableStatus = null };
        var json = Serialize(obj);
        json.Should().NotContain("\"nullableStatus\"");
    }

    [Fact]
    public void Serialize_NullableStringEnum_WithValue_WritesString()
    {
        var obj = new EnumWrapper { Status = TestStatus.Active, NullableStatus = TestStatus.Inactive };
        var json = Serialize(obj);
        json.Should().Contain("\"nullableStatus\":\"inactive\"");
    }

    [Fact]
    public void Serialize_NonStringEnum_WritesAsNumber()
    {
        var obj = new NumericEnumWrapper { Status = NumericStatus.Closed };
        var json = Serialize(obj);
        json.Should().Contain("\"status\":1");
    }

    private T Deserialize<T>(string json)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return _serializer.Deserialize<T>(stream);
    }

    private string Serialize<T>(T obj)
    {
        using var stream = new MemoryStream();
        _serializer.Serialize(obj, stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
