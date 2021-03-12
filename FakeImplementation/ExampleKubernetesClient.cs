using k8s;
using Microsoft.Rest;
using System;
using System.Net.Http;
using System.Runtime.ExceptionServices;
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

    public static class ExampleKubernetesExtensions
    {
        //
        // Summary:
        //     create a watch object from a call to api server with watch=true
        //
        // Parameters:
        //   responseTask:
        //     the api response
        //
        //   onEvent:
        //     a callback when any event raised from api server
        //
        //   onError:
        //     a callbak when any exception was caught during watching
        //
        //   onClosed:
        //     The action to invoke when the server closes the connection.
        //
        // Type parameters:
        //   T:
        //     type of the event object
        //
        //   L:
        //     type of the HttpOperationResponse object
        //
        // Returns:
        //     a watch object
        public static async Task WatchAsync<T, L>(this Task<HttpOperationResponse<L>> responseTask, Action<WatchEventType, T> onEvent, Action<Exception> onError = null, Action onClosed = null, CancellationToken cancellationToken = default)
        {
            Exception finalError = null;
            var tcs = new TaskCompletionSource();
            using var watcher = responseTask.Watch(onEvent, OnError, OnClosed);
            using var registration = cancellationToken.Register(watcher.Dispose);
            await tcs.Task;

            void OnError(Exception error)
            {
                try
                {
                    onError?.Invoke(error);
                }
                catch (Exception caughtError)
                {
                    finalError = caughtError;
                    throw;
                }
            }

            void OnClosed()
            {
                try
                {
                    onClosed?.Invoke();
                }
                catch (Exception caughtError)
                {
                    finalError = caughtError;
                }
                finally
                {
                    if (finalError != null)
                    {
                        tcs.SetException(finalError);
                    }
                    else
                    {
                        tcs.SetResult();
                    }
                }
            }
        }
    }
}
