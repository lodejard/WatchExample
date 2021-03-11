using k8s;
using Microsoft.Rest;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace WatchExample
{
    public class ExampleKubernetesClient : Kubernetes
    {
        public ExampleKubernetesClient(KubernetesClientConfiguration config, params DelegatingHandler[] handlers) : base(config, handlers)
        {
        }

        public async Task<WatchStream<T>> WatchAsync<T>(string resourceVersion = default, bool allowWatchBookmarks = default, CancellationToken cancellationToken = default) where T : new()
        {
            return (await WatchAsyncWithHttpMessagesAsync<T>(resourceVersion, allowWatchBookmarks, cancellationToken)).Body;
        }

        public async Task<HttpOperationResponse<WatchStream<T>>> WatchAsyncWithHttpMessagesAsync<T>(string resourceVersion = default, bool allowWatchBookmarks = default, CancellationToken cancellationToken = default) where T : new()
        {
            var client = new HttpClient(HttpClientHandler, disposeHandler: false);

            var request = new HttpRequestMessage
            {
                RequestUri = new Uri($"{BaseUri}api/v1/pods?allowWatchBookmarks={allowWatchBookmarks}&watch=true&resourceVersion={resourceVersion}"),
            };

            await Credentials.ProcessHttpRequestAsync(request, cancellationToken);

            var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken);

            return new HttpOperationResponse<WatchStream<T>>
            {
                Request = request,
                Response = response,
                Body = new WatchStream<T>(stream, response),
            };
        }
    }
}
