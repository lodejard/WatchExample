using k8s;
using Microsoft.Rest.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WatchExample
{
    public class WatchStream<T> : IAsyncEnumerable<(WatchEventType eventType, T resource)>, IDisposable where T : new()
    {
        private readonly StreamReader _reader;
        private readonly IDisposable _response;
        private readonly List<Memory<char>> _previousData = new List<Memory<char>>();
        private Memory<char> _currentBuffer = Memory<char>.Empty;
        private Memory<char> _currentData = Memory<char>.Empty;

        public WatchStream(Stream stream, IDisposable response)
        {
            _reader = new StreamReader(stream, leaveOpen: false);
            _response = response;
        }

        public void Dispose()
        {
            try
            {
                _reader.Dispose();
            }
            finally
            {
                _response.Dispose();
            }
        }

        public IAsyncEnumerator<(WatchEventType eventType, T resource)> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerator(this, cancellationToken);
        }

        public async Task<(WatchEventType eventType, T resource, bool connected)> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            if (TryGetLine(out var line))
            {
                var data = SafeJsonConvert.DeserializeObject<Watcher<T>.WatchEvent>(line);
                return (data.Type, data.Object, true);
            }

            while (true)
            {
                if (_currentBuffer.IsEmpty)
                {
                    _currentBuffer = new Memory<char>(new char[4096]);
                }
                if (!_currentData.IsEmpty)
                {
                    _previousData.Add(_currentData);
                    _currentData = Memory<char>.Empty;
                }

                var length = await _reader.ReadAsync(_currentBuffer, cancellationToken);
                if (length == 0)
                {
                    // stream closed
                    return (default, default, false);
                }

                _currentData = _currentBuffer.Slice(0, length);
                _currentBuffer = _currentBuffer.Slice(length);

                if (TryGetLine(out line))
                {
                    var data = SafeJsonConvert.DeserializeObject<Watcher<T>.WatchEvent>(line);
                    return (data.Type, data.Object, true);
                }
            }
        }

        private bool TryGetLine(out string line)
        {
            var delimiterIndex = _currentData.Span.IndexOf('\n');
            if (delimiterIndex == -1)
            {
                line = null;
                return false;
            }

            if (_previousData.Count != 0)
            {
                var sb = new StringBuilder();
                foreach (var buffer in _previousData)
                {
                    sb.Append(buffer);
                }
                _previousData.Clear();

                sb.Append(_currentData.Slice(0, delimiterIndex));
                _currentData = _currentData.Slice(delimiterIndex + 1);
                line = sb.ToString();
                return true;
            }

            line = new string(_currentData.Slice(0, delimiterIndex).Span);
            _currentData = _currentData.Slice(delimiterIndex + 1);
            return true;
        }

        internal class AsyncEnumerator : IAsyncEnumerator<(WatchEventType eventType, T resource)>
        {
            private WatchStream<T> _self;
            private CancellationToken _cancellationToken;

            public AsyncEnumerator(WatchStream<T> self, CancellationToken cancellationToken)
            {
                _self = self;
                _cancellationToken = cancellationToken;
            }

            public (WatchEventType eventType, T resource) Current { get; set; }

            public ValueTask DisposeAsync()
            {
                _self.Dispose();
                return ValueTask.CompletedTask;
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                var (eventType, resource, connected) = await _self.ReadNextAsync(_cancellationToken);
                Current = (eventType, resource);
                return connected;
            }
        }
    }
}
