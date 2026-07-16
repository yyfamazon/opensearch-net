/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.IO;
using System.Text;
using OpenSearch.Client;
using OpenSearch.Net;

namespace Tests.Serialization.Client;

/// <summary>
/// Helper shared across tests for serializing/deserializing via the OpenSearch.Client high-level serializer.
/// </summary>
public static class SerializerHelper
{
    public static IOpenSearchSerializer GetSerializer()
    {
        var pool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
        var settings = new ConnectionSettings(pool);
        return ((IConnectionSettingsValues)settings).RequestResponseSerializer;
    }

    public static string Serialize<T>(T obj)
    {
        var serializer = GetSerializer();
        using var stream = new MemoryStream();
        serializer.Serialize(obj, stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    public static T? Deserialize<T>(string json)
    {
        var serializer = GetSerializer();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return serializer.Deserialize<T>(stream);
    }
}
