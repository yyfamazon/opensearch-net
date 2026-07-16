/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/


using System;

using System.Buffers;

using System.IO;

using System.Text.Json;

using System.Text.Json.Serialization;

#nullable disable

namespace OpenSearch.Client.SystemTextJsonConverters;

/// <summary>
/// A converter factory that delegates serialization of user document types (T in generic APIs)
/// to the configured SourceSerializer. This replaces the SourceFormatter&lt;T&gt; from Utf8Json.
/// </summary>
internal sealed class SourceConverterFactory : JsonConverterFactory
{
	private readonly IConnectionSettingsValues _settings;

	public SourceConverterFactory(IConnectionSettingsValues settings) => _settings = settings;

	public override bool CanConvert(Type typeToConvert) =>
		// Only handle user document types (POCOs) - not primitives, not OpenSearch types
		!IsOpenSearchClientType(typeToConvert)
		&& !typeToConvert.IsPrimitive
		&& typeToConvert != typeof(string)
		&& typeToConvert != typeof(decimal)
		&& typeToConvert != typeof(object)
		&& !typeToConvert.IsEnum;

	public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
	{
		var converterType = typeof(SourceConverter<>).MakeGenericType(typeToConvert);
		return (JsonConverter)Activator.CreateInstance(converterType, _settings);
	}

	/// <summary>
	/// Determines if a type belongs to the OpenSearch.Client namespace and should NOT be
	/// handled by the source serializer.
	/// </summary>
	private static bool IsOpenSearchClientType(Type type)
	{
		var ns = type.Namespace;
		return ns != null && (ns.StartsWith("OpenSearch.Client", StringComparison.Ordinal)
			|| ns.StartsWith("OpenSearch.Net", StringComparison.Ordinal));
	}
}

internal sealed class SourceConverter<T> : JsonConverter<T>
{
	private readonly IConnectionSettingsValues _settings;
	private static readonly bool IsSimpleType = typeof(T).IsPrimitive
		|| typeof(T) == typeof(string) || typeof(T) == typeof(decimal)
		|| typeof(T).IsEnum || typeof(T) == typeof(object);

	public SourceConverter(IConnectionSettingsValues settings) => _settings = settings;

	public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		// For simple types, just let the default System.Text.Json handle it
		if (IsSimpleType)
			return JsonSerializer.Deserialize<T>(ref reader);

		// Capture the raw JSON for the current value
		using var document = JsonDocument.ParseValue(ref reader);
		using var ms = new MemoryStream();
		using (var jsonWriter = new Utf8JsonWriter(ms))
		{
			document.RootElement.WriteTo(jsonWriter);
		}

		ms.Position = 0;
		var sourceSerializer = _settings.SourceSerializer;

		// Avoid recursion: if SourceSerializer is the same as the request serializer,
		// deserialize without the SourceConverterFactory by using a plain deserialize
		if (sourceSerializer is DefaultHighLevelSystemTextJsonSerializer)
			return JsonSerializer.Deserialize<T>(ms);

		return sourceSerializer.Deserialize<T>(ms);
	}

	public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}

		// For simple types, just let the default System.Text.Json handle it
		if (IsSimpleType)
		{
			JsonSerializer.Serialize(writer, value);
			return;
		}

		var sourceSerializer = _settings.SourceSerializer;

		// Avoid recursion: if SourceSerializer is the same as the request serializer,
		// serialize using default options without the SourceConverterFactory
		if (sourceSerializer is DefaultHighLevelSystemTextJsonSerializer)
		{
			JsonSerializer.Serialize(writer, value);
			return;
		}

		using var stream = new MemoryStream();
		sourceSerializer.Serialize(value, stream);
		stream.Position = 0;

		// Write the raw JSON produced by the source serializer
		using var document = JsonDocument.Parse(stream);
		document.RootElement.WriteTo(writer);
	}
}
