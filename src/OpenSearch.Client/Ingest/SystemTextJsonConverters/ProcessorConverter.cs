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

namespace OpenSearch.Client.IngestConverters
{
	/// <summary>
	/// Polymorphic converter for <see cref="IProcessor"/>.
	/// Uses the type-as-key pattern where the JSON property name is the discriminator:
	/// <code>
	/// {"grok": {"field": "message", "patterns": ["%{COMBINEDAPACHELOG}"]}}
	/// {"rename": {"field": "hostname", "target_field": "host"}}
	/// </code>
	/// </summary>
	internal sealed class ProcessorConverter : JsonConverter<IProcessor>
	{
		public override IProcessor Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
		{
			if (reader.TokenType == JsonTokenType.Null)
				return null;

			if (reader.TokenType != JsonTokenType.StartObject)
				throw new JsonException($"Expected StartObject for IProcessor but got {reader.TokenType}");

			using var doc = JsonDocument.ParseValue(ref reader);
			var root = doc.RootElement;

			// The processor type is the single property name
			foreach (var property in root.EnumerateObject())
			{
				var processorType = property.Name;
				var rawJson = property.Value.GetRawText();
				return DeserializeProcessor(processorType, rawJson, options);
			}

			return null;
		}

		private static IProcessor DeserializeProcessor(string processorType, string rawJson, JsonSerializerOptions options)
		{
			switch (processorType)
			{
				case "attachment":
					return JsonSerializer.Deserialize<AttachmentProcessor>(rawJson, options);
				case "append":
					return JsonSerializer.Deserialize<AppendProcessor>(rawJson, options);
				case "convert":
					return JsonSerializer.Deserialize<ConvertProcessor>(rawJson, options);
				case "date":
					return JsonSerializer.Deserialize<DateProcessor>(rawJson, options);
				case "date_index_name":
					return JsonSerializer.Deserialize<DateIndexNameProcessor>(rawJson, options);
				case "dot_expander":
					return JsonSerializer.Deserialize<DotExpanderProcessor>(rawJson, options);
				case "fail":
					return JsonSerializer.Deserialize<FailProcessor>(rawJson, options);
				case "foreach":
					return JsonSerializer.Deserialize<ForeachProcessor>(rawJson, options);
				case "json":
					return JsonSerializer.Deserialize<JsonProcessor>(rawJson, options);
				case "user_agent":
					return JsonSerializer.Deserialize<UserAgentProcessor>(rawJson, options);
				case "kv":
					return JsonSerializer.Deserialize<KeyValueProcessor>(rawJson, options);
				case "geoip":
					return JsonSerializer.Deserialize<GeoIpProcessor>(rawJson, options);
				case "grok":
					return JsonSerializer.Deserialize<GrokProcessor>(rawJson, options);
				case "gsub":
					return JsonSerializer.Deserialize<GsubProcessor>(rawJson, options);
				case "join":
					return JsonSerializer.Deserialize<JoinProcessor>(rawJson, options);
				case "lowercase":
					return JsonSerializer.Deserialize<LowercaseProcessor>(rawJson, options);
				case "remove":
					return JsonSerializer.Deserialize<RemoveProcessor>(rawJson, options);
				case "rename":
					return JsonSerializer.Deserialize<RenameProcessor>(rawJson, options);
				case "script":
					return JsonSerializer.Deserialize<ScriptProcessor>(rawJson, options);
				case "set":
					return JsonSerializer.Deserialize<SetProcessor>(rawJson, options);
				case "sort":
					return JsonSerializer.Deserialize<SortProcessor>(rawJson, options);
				case "split":
					return JsonSerializer.Deserialize<SplitProcessor>(rawJson, options);
				case "trim":
					return JsonSerializer.Deserialize<TrimProcessor>(rawJson, options);
				case "uppercase":
					return JsonSerializer.Deserialize<UppercaseProcessor>(rawJson, options);
				case "urldecode":
					return JsonSerializer.Deserialize<UrlDecodeProcessor>(rawJson, options);
				case "bytes":
					return JsonSerializer.Deserialize<BytesProcessor>(rawJson, options);
				case "dissect":
					return JsonSerializer.Deserialize<DissectProcessor>(rawJson, options);
				case "pipeline":
					return JsonSerializer.Deserialize<PipelineProcessor>(rawJson, options);
				case "drop":
					return JsonSerializer.Deserialize<DropProcessor>(rawJson, options);
				case "csv":
					return JsonSerializer.Deserialize<CsvProcessor>(rawJson, options);
				case "uri_parts":
					return JsonSerializer.Deserialize<UriPartsProcessor>(rawJson, options);
				case "fingerprint":
					return JsonSerializer.Deserialize<FingerprintProcessor>(rawJson, options);
				case "community_id":
					return JsonSerializer.Deserialize<NetworkCommunityIdProcessor>(rawJson, options);
				case "network_direction":
					return JsonSerializer.Deserialize<NetworkDirectionProcessor>(rawJson, options);
				case "text_embedding":
					return JsonSerializer.Deserialize<TextEmbeddingProcessor>(rawJson, options);
				default:
					return null;
			}
		}

		public override void Write(Utf8JsonWriter writer, IProcessor value, JsonSerializerOptions options)
		{
			if (value == null)
			{
				writer.WriteNullValue();
				return;
			}

			writer.WriteStartObject();
			writer.WritePropertyName(value.Name);
			SerializeProcessorByInterfaceType(writer, value, options);
			writer.WriteEndObject();
		}

		/// <summary>
		/// Serializes the processor using its interface type rather than the concrete/descriptor type.
		/// This is critical because OpenSearch.Client uses explicit interface implementations for properties
		/// decorated with [DataMember], and STJ cannot discover explicit interface properties via the
		/// concrete runtime type.
		/// </summary>
		private static void SerializeProcessorByInterfaceType(Utf8JsonWriter writer, IProcessor value, JsonSerializerOptions options)
		{
			switch (value.Name)
			{
				case "attachment":
					JsonSerializer.Serialize(writer, (IAttachmentProcessor)value, options);
					break;
				case "append":
					JsonSerializer.Serialize(writer, (IAppendProcessor)value, options);
					break;
				case "convert":
					JsonSerializer.Serialize(writer, (IConvertProcessor)value, options);
					break;
				case "date":
					JsonSerializer.Serialize(writer, (IDateProcessor)value, options);
					break;
				case "date_index_name":
					JsonSerializer.Serialize(writer, (IDateIndexNameProcessor)value, options);
					break;
				case "dot_expander":
					JsonSerializer.Serialize(writer, (IDotExpanderProcessor)value, options);
					break;
				case "fail":
					JsonSerializer.Serialize(writer, (IFailProcessor)value, options);
					break;
				case "foreach":
					JsonSerializer.Serialize(writer, (IForeachProcessor)value, options);
					break;
				case "json":
					JsonSerializer.Serialize(writer, (IJsonProcessor)value, options);
					break;
				case "user_agent":
					JsonSerializer.Serialize(writer, (IUserAgentProcessor)value, options);
					break;
				case "kv":
					JsonSerializer.Serialize(writer, (IKeyValueProcessor)value, options);
					break;
				case "geoip":
					JsonSerializer.Serialize(writer, (IGeoIpProcessor)value, options);
					break;
				case "grok":
					JsonSerializer.Serialize(writer, (IGrokProcessor)value, options);
					break;
				case "gsub":
					JsonSerializer.Serialize(writer, (IGsubProcessor)value, options);
					break;
				case "join":
					JsonSerializer.Serialize(writer, (IJoinProcessor)value, options);
					break;
				case "lowercase":
					JsonSerializer.Serialize(writer, (ILowercaseProcessor)value, options);
					break;
				case "remove":
					JsonSerializer.Serialize(writer, (IRemoveProcessor)value, options);
					break;
				case "rename":
					JsonSerializer.Serialize(writer, (IRenameProcessor)value, options);
					break;
				case "script":
					JsonSerializer.Serialize(writer, (IScriptProcessor)value, options);
					break;
				case "set":
					JsonSerializer.Serialize(writer, (ISetProcessor)value, options);
					break;
				case "sort":
					JsonSerializer.Serialize(writer, (ISortProcessor)value, options);
					break;
				case "split":
					JsonSerializer.Serialize(writer, (ISplitProcessor)value, options);
					break;
				case "trim":
					JsonSerializer.Serialize(writer, (ITrimProcessor)value, options);
					break;
				case "uppercase":
					JsonSerializer.Serialize(writer, (IUppercaseProcessor)value, options);
					break;
				case "urldecode":
					JsonSerializer.Serialize(writer, (IUrlDecodeProcessor)value, options);
					break;
				case "bytes":
					JsonSerializer.Serialize(writer, (IBytesProcessor)value, options);
					break;
				case "dissect":
					JsonSerializer.Serialize(writer, (IDissectProcessor)value, options);
					break;
				case "pipeline":
					JsonSerializer.Serialize(writer, (IPipelineProcessor)value, options);
					break;
				case "drop":
					JsonSerializer.Serialize(writer, (IDropProcessor)value, options);
					break;
				case "csv":
					JsonSerializer.Serialize(writer, (ICsvProcessor)value, options);
					break;
				case "uri_parts":
					JsonSerializer.Serialize(writer, (IUriPartsProcessor)value, options);
					break;
				case "fingerprint":
					JsonSerializer.Serialize(writer, (IFingerprintProcessor)value, options);
					break;
				case "community_id":
					JsonSerializer.Serialize(writer, (INetworkCommunityIdProcessor)value, options);
					break;
				case "network_direction":
					JsonSerializer.Serialize(writer, (INetworkDirectionProcessor)value, options);
					break;
				case "text_embedding":
					JsonSerializer.Serialize(writer, (ITextEmbeddingProcessor)value, options);
					break;
				default:
					// Fallback: serialize via runtime type (may miss explicit interface properties)
					JsonSerializer.Serialize(writer, value, value.GetType(), options);
					break;
			}
		}
	}
}
