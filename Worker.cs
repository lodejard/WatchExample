using k8s;
using k8s.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace WatchExample
{
    class Worker
    {
        ExampleKubernetesClient _client = new ExampleKubernetesClient(k8s.KubernetesClientConfiguration.BuildDefaultConfig());

        private async Task<string> InitialListAsync(CancellationToken cancellationToken)
        {
            string resourceVersion;
            string continueProperty;

            do
            {
                var listPods = await _client.ListPodForAllNamespacesWithHttpMessagesAsync(
                    limit: 10, 
                    cancellationToken: cancellationToken);

                resourceVersion = listPods.Body.Metadata.ResourceVersion;
                continueProperty = listPods.Body.Metadata.ContinueProperty;
            }
            while (string.IsNullOrEmpty(continueProperty));

            return resourceVersion;
        }

        public async Task StyleOne(CancellationToken cancellationToken)
        {
            // outer loop: initial list, or re-list when resourceVersion reset by specific error
            while (!cancellationToken.IsCancellationRequested)
            {
                var resourceVersion = await InitialListAsync(cancellationToken);

                // middle loop: watch after list, or re-connect at last resourceVersion when response terminated
                while (!cancellationToken.IsCancellationRequested && !string.IsNullOrEmpty(resourceVersion))
                {
                    try
                    {
                        // Get list response
                        var listTask = _client.ListPodForAllNamespacesWithHttpMessagesAsync(
                            allowWatchBookmarks: true, 
                            watch: true, 
                            resourceVersion: resourceVersion, 
                            cancellationToken: cancellationToken);

                        // inner loop: receive items as lines arrive
                        var tcs = new TaskCompletionSource();
                        using var watcher = listTask.Watch<V1Pod, V1PodList>(OnEvent, OnError, OnClosed);
                        using var registration = cancellationToken.Register(watcher.Dispose);
                        await tcs.Task;

                        void OnClosed()
                        {
                            Console.WriteLine($"OnClosed");

                            tcs.TrySetResult();
                        }

                        void OnError(Exception error)
                        {
                            if (error is KubernetesException kubernetesError)
                            {
                                // deal with this non-recoverable condition "too old resource version"
                                if (string.Equals(kubernetesError.Status.Reason, "Expired", StringComparison.Ordinal))
                                {
                                    // force control back to outer loop
                                    resourceVersion = null;
                                }
                            }

                            Console.WriteLine($"OnError");

                            tcs.TrySetException(error);
                            throw error;
                        }

                        void OnEvent(WatchEventType eventType, V1Pod resource)
                        {
                            resourceVersion = resource.ResourceVersion();

                            Console.WriteLine($"OnEvent {eventType} {resource.Kind}/{resource.Name()}.{resource.Namespace()}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }

        public async Task StyleTwo(CancellationToken cancellationToken)
        {
            // outer loop: initial list, or re-list when resourceVersion reset by specific error
            while (!cancellationToken.IsCancellationRequested)
            {
                var resourceVersion = await InitialListAsync(cancellationToken);

                // middle loop: watch after list, or re-connect at last resourceVersion when response terminated
                while (!cancellationToken.IsCancellationRequested && !string.IsNullOrEmpty(resourceVersion))
                {
                    try
                    {
                        using var watch = await _client.WatchAsync<V1Pod>(
                            resourceVersion: resourceVersion, 
                            allowWatchBookmarks: true, 
                            cancellationToken: cancellationToken);

                        // inner loop: receive items as lines arrive
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            var (eventType, item, connected) = await watch.ReadNextAsync(cancellationToken);

                            if (!connected)
                            {
                                break;
                            }

                            resourceVersion = item.ResourceVersion();

                            Console.WriteLine($"ReadNextAsync {eventType} {item.Kind}/{item.Name()}.{item.Namespace()}");
                        }
                    }
                    catch (Exception error)
                    {
                        if (error is KubernetesException kubernetesError)
                        {
                            // deal with this non-recoverable condition "too old resource version"
                            if (string.Equals(kubernetesError.Status.Reason, "Expired", StringComparison.Ordinal))
                            {
                                // force control back to outer loop
                                resourceVersion = null;
                            }
                        }

                        Console.WriteLine(error.Message);
                    }
                }
            }
        }

        public async Task StyleThree(CancellationToken cancellationToken)
        {
            // outer loop: initial list, or re-list when resourceVersion reset by specific error
            while (!cancellationToken.IsCancellationRequested)
            {
                var resourceVersion = await InitialListAsync(cancellationToken);

                // middle loop: watch after list, or re-connect at last resourceVersion when response terminated
                while (!cancellationToken.IsCancellationRequested && !string.IsNullOrEmpty(resourceVersion))
                {
                    try
                    {
                        using var watch = await _client.WatchAsync<V1Pod>(
                            resourceVersion: resourceVersion, 
                            allowWatchBookmarks: true, 
                            cancellationToken: cancellationToken);

                        // inner loop: receive items as lines arrive
                        await foreach (var (eventType, item) in watch.WithCancellation(cancellationToken))
                        {
                            resourceVersion = item.ResourceVersion();

                            Console.WriteLine($"foreach {eventType} {item.Kind}/{item.Name()}.{item.Namespace()}");
                        }
                    }
                    catch (Exception error)
                    {
                        if (error is KubernetesException kubernetesError)
                        {
                            // deal with this non-recoverable condition "too old resource version"
                            if (string.Equals(kubernetesError.Status.Reason, "Expired", StringComparison.Ordinal))
                            {
                                // force control back to outer loop
                                resourceVersion = null;
                            }
                        }

                        Console.WriteLine(error.Message);
                    }
                }
            }
        }
    }
}
