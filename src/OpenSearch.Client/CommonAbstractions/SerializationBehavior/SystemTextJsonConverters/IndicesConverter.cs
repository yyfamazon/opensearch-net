/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using OpenSearch.Net;

#nullable disable

namespace OpenSearch.Client.SystemTextJsonConverters;

internal sealed class IndicesConverter : JsonConverter<Indices>
{
	private readonly IConnectionSettingsValues _settings;

	public IndicesConverter(IConnectionSettingsValues settings) => _settings = settings;

	public override Indices Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		if (reader.TokenType == JsonTokenType.Null) return null;
		if (reader.TokenType == JsonTokenType.String)
		{
			var str = reader.GetString();
			if (str == "_all" || str == "*") return Indices.All;
			return (Indices)str;
		}
		if (reader.TokenType == JsonTokenType.StartArray)
		{
			var indices = new List<IndexName>();
			while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
			{
				if (reader.TokenType == JsonTokenType.String)
					indices.Add((IndexName)reader.GetString());
			}
			return (Indices)indices.ToArray();
		}
		throw new JsonException($"Unexpected token {reader.TokenType} for Indices");
	}

	public override void Write(Utf8JsonWriter writer, Indices value, JsonSerializerOptions options)
	{
		if (value == null) { writer.WriteNullValue(); return; }

		var resolved = ((IUrlParameter)value).GetString(_settings);
		if (resolved == "_all" || resolved == "*")
		{
			writer.WriteStringValue(resolved);
			return;
		}

		// Could be comma-separated list or single index
		if (resolved.Contains(","))
		{
			writer.WriteStartArray();
			foreach (var idx in resolved.Split(','))
				writer.WriteStringValue(idx.Trim());
			writer.WriteEndArray();
		}
		else
		{
			writer.WriteStringValue(resolved);
		}
	}
}
