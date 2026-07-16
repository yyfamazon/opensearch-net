#nullable disable
/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/


using System;

using System.Text.Json;

using System.Text.Json.Serialization;

using OpenSearch.Net;

using OpenSearch.Client.SystemTextJsonConverters;

namespace OpenSearch.Client.QueryDsl.SystemTextJsonConverters
{
	/// <summary>
	/// Converter for <see cref="ITermQuery"/>.
	/// JSON format: <c>{"field": {"value": "...", "boost": 1.0, "case_insensitive": false}}</c>
	/// Also supports short form: <c>{"field": "value"}</c> or <c>{"field": 123}</c>
	/// </summary>
	internal sealed class TermQueryConverter : FieldNameQueryConverterBase<ITermQuery, TermQuery>
	{
		protected override void ReadInnerProperties(ref Utf8JsonReader reader, TermQuery query, JsonSerializerOptions options)
		{
			if (reader.TokenType != JsonTokenType.StartObject)
				throw new JsonException("Expected StartObject for term query inner properties");

			while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
			{
				if (reader.TokenType != JsonTokenType.PropertyName)
					throw new JsonException("Expected PropertyName token");

				var propertyName = reader.GetString();
				reader.Read(); // Move to value

				switch (propertyName)
				{
					case "value":
						query.Value = ReadValue(ref reader);
						break;
					case "boost":
						query.Boost = reader.GetDouble();
						break;
					case "case_insensitive":
						query.CaseInsensitive = reader.GetBoolean();
						break;
					case "_name":
						query.Name = reader.GetString();
						break;
					default:
						reader.Skip();
						break;
				}
			}
		}

		protected override void WriteInnerProperties(Utf8JsonWriter writer, ITermQuery value, JsonSerializerOptions options)
		{
			if (value.Value != null)
			{
				writer.WritePropertyName("value");
				WriteValue(writer, value.Value, options);
			}

			if (value.CaseInsensitive.HasValue)
			{
				writer.WritePropertyName("case_insensitive");
				writer.WriteBooleanValue(value.CaseInsensitive.Value);
			}
		}

		protected override void ReadShortForm(ref Utf8JsonReader reader, TermQuery query, JsonSerializerOptions options)
		{
			// Short form: {"field": "value"} or {"field": 123}
			query.Value = ReadValue(ref reader);
		}

		private static object ReadValue(ref Utf8JsonReader reader)
		{
			switch (reader.TokenType)
			{
				case JsonTokenType.String:
					return reader.GetString();
				case JsonTokenType.Number:
					if (reader.TryGetInt64(out var longVal))
						return longVal;
					return reader.GetDouble();
				case JsonTokenType.True:
					return true;
				case JsonTokenType.False:
					return false;
				case JsonTokenType.Null:
					return null;
				default:
					throw new JsonException($"Unexpected token type {reader.TokenType} for term query value");
			}
		}

		private static void WriteValue(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
		{
			switch (value)
			{
				case string s:
					writer.WriteStringValue(s);
					break;
				case int i:
					writer.WriteNumberValue(i);
					break;
				case long l:
					writer.WriteNumberValue(l);
					break;
				case double d:
					writer.WriteNumberValue(d);
					break;
				case float f:
					writer.WriteNumberValue(f);
					break;
				case decimal dec:
					writer.WriteNumberValue(dec);
					break;
				case bool b:
					writer.WriteBooleanValue(b);
					break;
				case null:
					writer.WriteNullValue();
					break;
				case IUrlParameter urlParam:
					// Handles RelationName, IndexName, Id, Routing etc. which implement IUrlParameter
					var settings = GetConnectionSettings(options);
					writer.WriteStringValue(urlParam.GetString(settings));
					break;
				default:
					writer.WriteStringValue(value.ToString());
					break;
			}
		}

		private static IConnectionConfigurationValues GetConnectionSettings(JsonSerializerOptions options)
		{
			foreach (var converter in options.Converters)
			{
				if (converter is FieldConverter fc)
					return fc.Settings;
			}
			return null;
		}
	}
}
