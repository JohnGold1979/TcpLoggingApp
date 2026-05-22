using System;

namespace TcpLoggingApp.Server
{
    internal sealed class StringBuilderPool : IDisposable
    {
        private char[]? _buffer;
        private int _length;
        private const int DefaultCapacity = 256;

        public int Length => _length;

        public StringBuilderPool()
        {
            _buffer = new char[DefaultCapacity];
            _length = 0;
        }

        public void Append(ReadOnlySpan<char> value)
        {
            if (_buffer == null)
                throw new ObjectDisposedException(nameof(StringBuilderPool));

            var newLength = _length + value.Length;
            if (newLength > _buffer.Length)
            {
                var newSize = Math.Max(_buffer.Length * 2, newLength);
                Array.Resize(ref _buffer, newSize);
            }

            value.CopyTo(_buffer.AsSpan(_length));
            _length = newLength;
        }

        public override string ToString()
        {
            if (_buffer == null)
                return string.Empty;
            return new string(_buffer, 0, _length);
        }

        public void Clear()
        {
            _length = 0;
        }

        public void Dispose()
        {
            _buffer = null;
            _length = 0;
        }
    }
}