using k8s;
using k8s.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace WatchExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();

            var task1 = new Worker().StyleOne(cts.Token);
            var task1_5 = new Worker().StyleOnePointFive(cts.Token);
            var task2 = new Worker().StyleTwo(cts.Token);
            var task3 = new Worker().StyleThree(cts.Token);

            await Task.WhenAll(task1, task1_5, task2, task3);
        }
    }
}
