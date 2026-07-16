#nullable disable
/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/
/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*
*  Licensed to Elasticsearch B.V. under one or more contributor
*  license agreements. See the NOTICE file distributed with
*  this work for additional information regarding copyright
*  ownership. Elasticsearch B.V. licenses this file to you under
*  the Apache License, Version 2.0 (the "License"); you may
*  not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
* 	http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/


using System;

using System.IO;

using System.Text.Encodings.Web;

using System.Text.Json;

using System.Text.Json.Serialization;

using System.Threading;

using System.Threading.Tasks;

using OpenSearch.Net;

using OpenSearch.Net.Serialization.Converters;

using OpenSearch.Client.IngestConverters;
using OpenSearch.Client.QueryDsl.SystemTextJsonConverters;
using OpenSearch.Client.Aggregations.SystemTextJsonConverters;
using OpenSearch.Client.CommonOptions.SystemTextJsonConverters;
using OpenSearch.Client.SystemTextJsonConverters;
using OpenSearch.Client.Mapping.SystemTextJsonConverters;
using OpenSearch.Client.AnalysisConverters;
using OpenSearch.Client.IndexManagement.SystemTextJsonConverters;
using OpenSearch.Client.Document.SystemTextJsonConverters;
using OpenSearch.Client.Search.SystemTextJsonConverters;
using OpenSearch.Client.Cluster.SystemTextJsonConverters;
using OpenSearch.Client.SnapshotConverters;

namespace OpenSearch.Client
{
	/// <summary>
	/// The built-in internal serializer that the high level client OpenSearch.Client uses,
	/// based on System.Text.Json. Replaces the legacy Utf8Json-based <see cref="DefaultHighLevelSerializer"/>.
	/// </summary>
	internal class DefaultHighLevelSystemTextJsonSerializer : IOpenSearchSerializer
	{
		private readonly JsonSerializerOptions _options;

		/// <summary>
		/// The connection settings values used by converters that need access to property mappings,
		/// field name inferrer, and other client configuration.
		/// </summary>
		public IConnectionSettingsValues Settings { get; }

		/// <summary>
		/// The source serializer used for user document (POCO) handling.
		/// This is set after construction since there may be a circular dependency
		/// between the request serializer and the source serializer.
		/// </summary>
		public IOpenSearchSerializer SourceSerializer { get; set; }

		public DefaultHighLevelSystemTextJsonSerializer(IConnectionSettingsValues settings)
		{
			Settings = settings ?? throw new ArgumentNullException(nameof(settings));

			_options = new JsonSerializerOptions
			{
				PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
				DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
				PropertyNameCaseInsensitive = true,
				NumberHandling = JsonNumberHandling.AllowReadingFromString,
				Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
				TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver
				{
					Modifiers =
					{
						typeInfo =>
						{
							if (typeInfo.Kind != System.Text.Json.Serialization.Metadata.JsonTypeInfoKind.Object)
								return;

							foreach (var prop in typeInfo.Properties)
							{
								var member = prop.AttributeProvider;
								if (member == null) continue;

								// System.Type cannot be serialized by STJ - always exclude
								if (prop.PropertyType == typeof(Type) || prop.PropertyType == typeof(System.Type))
								{
									prop.ShouldSerialize = static (_, _) => false;
									continue;
								}

								// Respect [IgnoreDataMember] - exclude from serialization
								// Check both the concrete member and interface declarations
								if (HasIgnoreDataMember(member, typeInfo.Type))
								{
									prop.ShouldSerialize = static (_, _) => false;
									continue;
								}

								// Respect [DataMember(Name = "...")] for property name mapping
								var dataMemberAttrs = member.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);
								if (dataMemberAttrs.Length > 0)
								{
									var dmAttr = (System.Runtime.Serialization.DataMemberAttribute)dataMemberAttrs[0];
									if (!string.IsNullOrEmpty(dmAttr.Name))
										prop.Name = dmAttr.Name;
								}
								else if (member is System.Reflection.PropertyInfo pi)
								{
									// Check interface declarations for [DataMember(Name = "...")]
									foreach (var iface in typeInfo.Type.GetInterfaces())
									{
										var ifaceProp = iface.GetProperty(pi.Name);
										if (ifaceProp == null) continue;
										var ifaceAttrs = ifaceProp.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);
										if (ifaceAttrs.Length > 0)
										{
											var ifaceDmAttr = (System.Runtime.Serialization.DataMemberAttribute)ifaceAttrs[0];
											if (!string.IsNullOrEmpty(ifaceDmAttr.Name))
											{
												prop.Name = ifaceDmAttr.Name;
												break;
											}
										}
									}
								}
							}

							// Add internal/private setter support for deserialization
							// This handles properties like `public bool Acknowledged { get; internal set; }`
							foreach (var prop in typeInfo.Properties)
							{
								if (prop.Set != null) continue; // already has a setter

								if (prop.AttributeProvider is System.Reflection.PropertyInfo propertyInfo
									&& propertyInfo.CanWrite)
								{
									var setter = propertyInfo.GetSetMethod(true);
									if (setter != null)
										prop.Set = (obj, val) => setter.Invoke(obj, new[] { val });
								}
							}
						},
						typeInfo =>
						{
							if (typeInfo.Kind != System.Text.Json.Serialization.Metadata.JsonTypeInfoKind.Object)
								return;

							// Discover non-public properties with [DataMember] attribute
							var bindingFlags = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
							foreach (var prop in typeInfo.Type.GetProperties(bindingFlags))
							{
								var dataMemberAttrs = prop.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);
								if (dataMemberAttrs.Length == 0) continue;

								// Skip if already present
								var dmAttr = (System.Runtime.Serialization.DataMemberAttribute)dataMemberAttrs[0];
								var jsonName = !string.IsNullOrEmpty(dmAttr.Name) ? dmAttr.Name : JsonNamingPolicy.CamelCase.ConvertName(prop.Name);

								bool alreadyExists = false;
								foreach (var existing in typeInfo.Properties)
								{
									if (existing.Name == jsonName) { alreadyExists = true; break; }
								}
								if (alreadyExists) continue;

								var jsonProp = typeInfo.CreateJsonPropertyInfo(prop.PropertyType, jsonName);
								jsonProp.Get = obj => prop.GetValue(obj);
								var setter = prop.GetSetMethod(true);
								if (setter != null)
									jsonProp.Set = (obj, val) => setter.Invoke(obj, new[] { val });
								typeInfo.Properties.Add(jsonProp);
							}
						}
					}
				}
			};

			// Register domain-specific converters.
			// Parameterless converters (polymorphic type-as-key serializers)
			_options.Converters.Add(new ProcessorConverter());
			_options.Converters.Add(new QueryContainerConverter());
			_options.Converters.Add(new QueryContainerInterfaceConverter());
			_options.Converters.Add(new QueryContainerCollectionConverter());
			_options.Converters.Add(new BoolQueryConverter());
			_options.Converters.Add(new TermQueryConverter());
			_options.Converters.Add(new TermsQueryConverter(settings));
			_options.Converters.Add(new MatchQueryConverter());
			_options.Converters.Add(new RangeQueryConverter());
			_options.Converters.Add(new NestedQueryConverter());
			_options.Converters.Add(new GeoDistanceQueryConverter());
			_options.Converters.Add(new GeoShapeQueryConverter());
			_options.Converters.Add(new DistanceFeatureQueryConverter());
			_options.Converters.Add(new FunctionScoreQueryConverter());
			_options.Converters.Add(new MoreLikeThisQueryConverter());
			_options.Converters.Add(new PercolateQueryConverter());
			_options.Converters.Add(new SpanQueryConverter());
			_options.Converters.Add(new ScoreFunctionConverter());
			_options.Converters.Add(new SimpleQueryStringFlagsConverter());
			_options.Converters.Add(new LikeConverter());
			_options.Converters.Add(new GeoLocationConverter());
			_options.Converters.Add(new GeoShapeConverter());
			_options.Converters.Add(new PropertyConverterFactory());
			_options.Converters.Add(new GeoOrientationConverter());
			_options.Converters.Add(new DynamicMappingConverter());
			_options.Converters.Add(new SuggestContextConverter());

			// Settings-aware converters
			_options.Converters.Add(new AggregationContainerConverter(settings));
			_options.Converters.Add(new AggregationContainerInterfaceConverter(settings));
			_options.Converters.Add(new AggregateConverter(settings));
			_options.Converters.Add(new AggregateDictionaryConverter(settings));
			_options.Converters.Add(new AggregationDictionaryConverter(settings));
			_options.Converters.Add(new SortConverter(settings));
			_options.Converters.Add(new OpenSearch.Client.CommonOptions.SystemTextJsonConverters.ScriptConverter(settings));
			_options.Converters.Add(new FuzzinessConverter(settings));
			_options.Converters.Add(new MinimumShouldMatchConverter(settings));
			_options.Converters.Add(new OpenSearch.Client.CommonOptions.SystemTextJsonConverters.DistanceConverter(settings));
			_options.Converters.Add(new DateMathConverter(settings));
			_options.Converters.Add(new DateMathExpressionConverter(settings));
			_options.Converters.Add(new StringBooleanConverter(settings));
			_options.Converters.Add(new TimeSpanTicksConverter(settings));
			_options.Converters.Add(new FieldConverter(settings));
			_options.Converters.Add(new FieldsConverter(settings));
			_options.Converters.Add(new IdConverter(settings));
			_options.Converters.Add(new IndexNameConverter(settings));
			_options.Converters.Add(new IndicesConverter(settings));
			_options.Converters.Add(new RelationNameConverter(settings));
			_options.Converters.Add(new RoutingConverter(settings));
			_options.Converters.Add(new LazyDocumentConverter(settings));
			_options.Converters.Add(new LazyDocumentInterfaceConverter(settings));
			_options.Converters.Add(new TimeConverter());
			_options.Converters.Add(new UnionConverterFactory());
			_options.Converters.Add(new PropertyNameConverter(settings));
			_options.Converters.Add(new PropertiesConverterFactory(settings));
			_options.Converters.Add(new ChildrenConverter(settings));
			_options.Converters.Add(new JoinFieldConverter(settings));
			_options.Converters.Add(new DynamicTemplateContainerInterfaceConverter());

			// Analysis converters
			_options.Converters.Add(new AnalyzerConverter());
			_options.Converters.Add(new TokenFilterConverter());
			_options.Converters.Add(new CharFilterConverter());
			_options.Converters.Add(new TokenizerConverter());
			_options.Converters.Add(new NormalizerConverter());
			_options.Converters.Add(new StopWordsConverter());

			// Index settings
			_options.Converters.Add(new IndexSettingsConverter());
			_options.Converters.Add(new AutoExpandReplicasConverter());
			_options.Converters.Add(new AliasConverter());
			_options.Converters.Add(new SimilarityConverter());

			// Document converters
			_options.Converters.Add(new SourceFilterConverter(settings));
			_options.Converters.Add(new ReindexRoutingConverter(settings));
			_options.Converters.Add(new SlicesConverter(settings));
			_options.Converters.Add(new BulkResponseItemConverter(settings));
			_options.Converters.Add(new IndexRequestConverterFactory(settings));
			_options.Converters.Add(new CreateRequestConverterFactory(settings));

			// Search converters
			_options.Converters.Add(new TotalHitsConverter());
			_options.Converters.Add(new TrackTotalHitsConverter());
			_options.Converters.Add(new HighlightConverter());
			_options.Converters.Add(new SuggestDictionaryConverterFactory());

			// Cluster converters
			_options.Converters.Add(new ClusterRerouteCommandConverter());

			// Snapshot converters
			_options.Converters.Add(new GetRepositoryResponseConverter());
			_options.Converters.Add(new CreateRepositoryConverter());

			// OpenSearchClientConverterFactory as fallback for any remaining types
			_options.Converters.Add(new OpenSearchClientConverterFactory(settings));

			// Low-level converters from OpenSearch.Net
			_options.Converters.Add(new DynamicDictionaryConverter());
			_options.Converters.Add(new NullableStringIntConverter());
			_options.Converters.Add(new StringEnumConverterFactory());
		}

		/// <summary>
		/// Returns the configured <see cref="JsonSerializerOptions"/> for use by converters
		/// that need to perform sub-serialization.
		/// </summary>
		public JsonSerializerOptions GetJsonSerializerOptions() => _options;

		/// <summary>
		/// Checks if a property has [IgnoreDataMember] either on the concrete class member
		/// or on the same-named property in any implemented interface.
		/// </summary>
		private static bool HasIgnoreDataMember(System.Reflection.ICustomAttributeProvider member, Type declaringType)
		{
			// Direct check on the concrete member
			if (member.IsDefined(typeof(System.Runtime.Serialization.IgnoreDataMemberAttribute), true))
				return true;

			// Check interface declarations for the same property name
			// GetInterfaceMap cannot be called on interface types themselves
			if (declaringType.IsInterface)
				return false;

			if (member is System.Reflection.PropertyInfo propInfo)
			{
				foreach (var iface in declaringType.GetInterfaces())
				{
					var ifaceProp = iface.GetProperty(propInfo.Name);
					if (ifaceProp != null && ifaceProp.IsDefined(typeof(System.Runtime.Serialization.IgnoreDataMemberAttribute), true))
						return true;
				}
			}

			return false;
		}

		/// <inheritdoc />
		public T Deserialize<T>(Stream stream)
		{
			if (stream == null || stream == Stream.Null) return default;
			if (stream.CanSeek && stream.Length == 0) return default;
			if (!stream.CanSeek)
			{
				// Buffer non-seekable streams to check for empty content
				var ms = new MemoryStream();
				stream.CopyTo(ms);
				if (ms.Length == 0) return default;
				ms.Position = 0;
				stream = ms;
			}

			var targetType = typeof(T);
			if (targetType.IsInterface)
			{
				var readAsAttrs = targetType.GetCustomAttributes(typeof(ReadAsAttribute), true);
				if (readAsAttrs.Length > 0)
				{
					var readAs = (ReadAsAttribute)readAsAttrs[0];
					var concreteType = readAs.Type;
					if (concreteType.IsGenericTypeDefinition)
						concreteType = concreteType.MakeGenericType(targetType.GetGenericArguments());
					return (T)JsonSerializer.Deserialize(stream, concreteType, _options);
				}
			}

			return JsonSerializer.Deserialize<T>(stream, _options);
		}

		/// <inheritdoc />
		public object Deserialize(Type type, Stream stream)
		{
			if (stream == null || stream == Stream.Null) return null;
			if (stream.CanSeek && stream.Length == 0) return null;
			if (!stream.CanSeek)
			{
				// Buffer non-seekable streams to check for empty content
				var ms = new MemoryStream();
				stream.CopyTo(ms);
				if (ms.Length == 0) return null;
				ms.Position = 0;
				stream = ms;
			}

			if (type.IsInterface)
			{
				var readAsAttrs = type.GetCustomAttributes(typeof(ReadAsAttribute), true);
				if (readAsAttrs.Length > 0)
				{
					var readAs = (ReadAsAttribute)readAsAttrs[0];
					var concreteType = readAs.Type;
					if (concreteType.IsGenericTypeDefinition)
						concreteType = concreteType.MakeGenericType(type.GetGenericArguments());
					return JsonSerializer.Deserialize(stream, concreteType, _options);
				}
			}

			return JsonSerializer.Deserialize(stream, type, _options);
		}

		/// <inheritdoc />
		public async Task<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
		{
			if (stream == null || stream == Stream.Null) return default;
			if (stream.CanSeek && stream.Length == 0) return default;
			if (!stream.CanSeek)
			{
				// Buffer non-seekable streams to check for empty content
				var ms = new MemoryStream();
				await stream.CopyToAsync(ms).ConfigureAwait(false);
				if (ms.Length == 0) return default;
				ms.Position = 0;
				stream = ms;
			}

			var targetType = typeof(T);
			if (targetType.IsInterface)
			{
				var readAsAttrs = targetType.GetCustomAttributes(typeof(ReadAsAttribute), true);
				if (readAsAttrs.Length > 0)
				{
					var readAs = (ReadAsAttribute)readAsAttrs[0];
					var concreteType = readAs.Type;
					if (concreteType.IsGenericTypeDefinition)
						concreteType = concreteType.MakeGenericType(targetType.GetGenericArguments());
					return (T)await JsonSerializer.DeserializeAsync(stream, concreteType, _options, cancellationToken).ConfigureAwait(false);
				}
			}

			return await JsonSerializer.DeserializeAsync<T>(stream, _options, cancellationToken).ConfigureAwait(false);
		}

		/// <inheritdoc />
		public async Task<object> DeserializeAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
		{
			if (stream == null || stream == Stream.Null) return null;
			if (stream.CanSeek && stream.Length == 0) return null;
			if (!stream.CanSeek)
			{
				// Buffer non-seekable streams to check for empty content
				var ms = new MemoryStream();
				await stream.CopyToAsync(ms).ConfigureAwait(false);
				if (ms.Length == 0) return null;
				ms.Position = 0;
				stream = ms;
			}

			if (type.IsInterface)
			{
				var readAsAttrs = type.GetCustomAttributes(typeof(ReadAsAttribute), true);
				if (readAsAttrs.Length > 0)
				{
					var readAs = (ReadAsAttribute)readAsAttrs[0];
					var concreteType = readAs.Type;
					if (concreteType.IsGenericTypeDefinition)
						concreteType = concreteType.MakeGenericType(type.GetGenericArguments());
					return await JsonSerializer.DeserializeAsync(stream, concreteType, _options, cancellationToken).ConfigureAwait(false);
				}
			}

			return await JsonSerializer.DeserializeAsync(stream, type, _options, cancellationToken).ConfigureAwait(false);
		}

		/// <inheritdoc />
		public void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None)
		{
			if (stream == null) throw new ArgumentNullException(nameof(stream));

			// Always write compact JSON for request bodies sent to the server.
			// The formatting parameter is intentionally ignored - indented output
			// would change content-length and break content hashes (e.g. SigV4).
			using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions
			{
				Indented = false,
				Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
			});
			JsonSerializer.Serialize(writer, data, _options);
			writer.Flush();
		}

		/// <inheritdoc />
		public Task SerializeAsync<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None,
			CancellationToken cancellationToken = default)
		{
			if (stream == null) throw new ArgumentNullException(nameof(stream));
			return JsonSerializer.SerializeAsync(stream, data, _options, cancellationToken);
		}
	}
}
