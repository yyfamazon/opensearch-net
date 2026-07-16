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

namespace OpenSearch.Client.Mapping.SystemTextJsonConverters
{
	/// <summary>
	/// Converter factory for <see cref="IProperties"/> and types implementing it (like <see cref="Properties"/>).
	/// JSON: {"field1": {"type": "text", ...}, "field2": {"type": "keyword", ...}}
	/// </summary>
	internal sealed class PropertiesConverterFactory : JsonConverterFactory
	{
		private readonly IConnectionSettingsValues _settings;

		public PropertiesConverterFactory(IConnectionSettingsValues settings) => _settings = settings;

		public override bool CanConvert(Type typeToConvert) =>
			typeof(IProperties).IsAssignableFrom(typeToConvert);

		public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options) =>
			new PropertiesConverter(_settings);
	}

	internal sealed class PropertiesConverter : JsonConverter<IProperties>
	{
		private readonly IConnectionSettingsValues _settings;

		public PropertiesConverter(IConnectionSettingsValues settings) => _settings = settings;

		public override IProperties Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
		{
			if (reader.TokenType == JsonTokenType.Null)
				return null;

			if (reader.TokenType != JsonTokenType.StartObject)
				throw new JsonException($"Expected StartObject for IProperties but got {reader.TokenType}");

			var properties = _settings != null ? new Properties(_settings) : new Properties();
			var propertyConverter = (JsonConverter<IProperty>)options.GetConverter(typeof(IProperty));

			while (reader.Read())
			{
				if (reader.TokenType == JsonTokenType.EndObject)
					break;

				if (reader.TokenType != JsonTokenType.PropertyName)
					throw new JsonException("Expected PropertyName in properties object");

				var fieldName = reader.GetString();
				reader.Read();

				if (reader.TokenType != JsonTokenType.StartObject)
				{
					reader.Skip();
					continue;
				}

				var property = propertyConverter.Read(ref reader, typeof(IProperty), options);
				if (property != null)
				{
					property.Name = fieldName;
					properties.Add(fieldName, property);
				}
			}

			return properties;
		}

		public override void Write(Utf8JsonWriter writer, IProperties value, JsonSerializerOptions options)
		{
			if (value == null)
			{
				writer.WriteNullValue();
				return;
			}

			var propertyConverter = (JsonConverter<IProperty>)options.GetConverter(typeof(IProperty));

			writer.WriteStartObject();

			// Use a HashSet to track already-written property names and prevent duplicates.
			// Duplicates can occur when auto-mapped properties and explicit properties both
			// appear in the IProperties dictionary (explicit should win, written first).
			var written = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);

			foreach (var kvp in value)
			{
				var propertyName = kvp.Key;
				var property = kvp.Value;

				string name;
				if (_settings != null)
				{
					try
					{
						name = _settings.Inferrer.PropertyName(propertyName) ?? propertyName.Name;
					}
					catch (System.ArgumentException)
					{
						name = propertyName.Name;
					}
				}
				else
				{
					name = propertyName.Name;
				}

				if (string.IsNullOrEmpty(name))
					continue;

				// Skip duplicates - first occurrence wins
				if (!written.Add(name))
					continue;

				writer.WritePropertyName(name);
				propertyConverter.Write(writer, property, options);
			}

			writer.WriteEndObject();
		}
	}
}
