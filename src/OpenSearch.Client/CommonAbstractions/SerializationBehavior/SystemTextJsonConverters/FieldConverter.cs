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

internal sealed class FieldConverter : JsonConverter<Field>
{
	private readonly IConnectionSettingsValues _settings;

	public FieldConverter(IConnectionSettingsValues settings) => _settings = settings;

	internal IConnectionSettingsValues Settings => _settings;

	public override Field Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		if (reader.TokenType == JsonTokenType.Null)
			return null;

		var fieldName = reader.GetString();
		return fieldName == null ? null : new Field(fieldName);
	}

	public override void Write(Utf8JsonWriter writer, Field value, JsonSerializerOptions options)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}

		var fieldName = value.Name ?? _settings.Inferrer.Field(value);
		writer.WriteStringValue(fieldName);
	}

	public override Field ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		var fieldName = reader.GetString();
		return fieldName == null ? null : new Field(fieldName);
	}

	public override void WriteAsPropertyName(Utf8JsonWriter writer, Field value, JsonSerializerOptions options)
	{
		var fieldName = value?.Name ?? (value != null ? _settings.Inferrer.Field(value) : "");
		writer.WritePropertyName(fieldName ?? "");
	}
}
