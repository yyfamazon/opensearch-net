/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Text.Json;
using System.Text.Json.Serialization;

#nullable disable

namespace OpenSearch.Client.SystemTextJsonConverters;

/// <summary>
/// Converter for <see cref="PropertyName"/> that supports both value serialization
/// and dictionary key serialization (ReadAsPropertyName/WriteAsPropertyName).
/// </summary>
internal sealed class PropertyNameConverter : JsonConverter<PropertyName>
{
	private readonly IConnectionSettingsValues _settings;

	public PropertyNameConverter(IConnectionSettingsValues settings) => _settings = settings;

	public override PropertyName Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		if (reader.TokenType == JsonTokenType.Null)
			return null;

		var name = reader.GetString();
		return name == null ? null : new PropertyName(name);
	}

	public override void Write(Utf8JsonWriter writer, PropertyName value, JsonSerializerOptions options)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}

		writer.WriteStringValue(Resolve(value));
	}

	public override PropertyName ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		var name = reader.GetString();
		return name == null ? null : new PropertyName(name);
	}

	public override void WriteAsPropertyName(Utf8JsonWriter writer, PropertyName value, JsonSerializerOptions options)
	{
		writer.WritePropertyName(Resolve(value));
	}

	private string Resolve(PropertyName value)
	{
		if (_settings != null)
			return _settings.Inferrer.PropertyName(value);

		return value.Name ?? value.Property?.Name ?? value.ToString();
	}
}
