using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Tiddly.Tests
{
    public class TiddlyCsvTests : IDisposable
    {
        public TiddlyCsvTests()
        {
            stream = File.Open("data/smalltest.csv", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            reader = new TiddlyCsvReader(stream);
        }

        public void Dispose()
        {
            stream.Close();
        }

        [Fact] //(Timeout = 10000)]
        public void Should_read_first_value_correctly()
        {
            var value = reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));

            Assert.Equal("a ", value);
        }

        [Fact] //(Timeout = 10000)]
        public void Should_read_second_value_correctly()
        {
            reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));
            var value = reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));

            Assert.Equal("b", value);
        }

        [Fact] //(Timeout = 10000)]
        public void Should_read_third_value_correctly()
        {
            reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));
            reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));
            var value = reader.EndReadNextValue(
                reader.BeginReadNextValue(null, null));

            Assert.Equal("c", value);
        }

        [Fact]
        public void Should_read_forth_value_null()
        {
            string value = "Fail";
            var waitHandle = new ManualResetEvent(false);
            reader.BeginReadNextValue((ar1) =>
            {
                reader.EndReadNextValue(ar1);
                reader.BeginReadNextValue((ar2) =>
                {
                    reader.EndReadNextValue(ar2);
                    reader.BeginReadNextValue((ar3) =>
                    {
                        reader.EndReadNextValue(ar3);
                        reader.BeginReadNextValue((ar4) =>
                        {
                            value = reader.EndReadNextValue(ar4);
                            waitHandle.Set();
                        }, null);
                    }, null);
                }, null);
            }, null);

            waitHandle.WaitOne(4000);

            Assert.Equal(null, value);
        }

        [Fact]
        public void Should_read_to_next_row_and_read_first_column()
        {
            string value = "Fail";
            var waitHandle = new ManualResetEvent(false);
            reader.BeginMoveToNextRow((nar) =>
                {
                    reader.EndMoveToNextRow(nar);
                    reader.BeginReadNextValue((rar) =>
                        {
                            value = reader.EndReadNextValue(rar);
                            waitHandle.Set();
                        },
                        null);
                },
                null);

            waitHandle.WaitOne(4000);
            Assert.Equal("1", value);
        }

        [Fact]
        public void Should_read_third_row_first_cell_correctly()
        {
            string value = "Fail";
            var waitHandle = new ManualResetEvent(false);
            reader.BeginMoveToNextRow((nar) =>
            {
                reader.EndMoveToNextRow(nar);
                reader.BeginMoveToNextRow((nar2) =>
                {
                    reader.EndMoveToNextRow(nar2);
                    reader.BeginReadNextValue((rar) =>
                    {
                        value = reader.EndReadNextValue(rar);
                        waitHandle.Set();
                    },
                    null);
                },
                null);
            },
            null);

            waitHandle.WaitOne(4000);
            Assert.Equal(String.Empty, value);
        }

        [Fact]
        public void Should_read_third_row_second_cell_correctly()
        {
            string value = "Fail";
            var waitHandle = new ManualResetEvent(false);
            reader.BeginMoveToNextRow((nar) =>
            {
                reader.EndMoveToNextRow(nar);
                reader.BeginMoveToNextRow((nar2) =>
                {
                    reader.EndMoveToNextRow(nar2);
                    reader.BeginReadNextValue((rar) =>
                    {
                        reader.BeginReadNextValue((rar2) =>
                        {
                            value = reader.EndReadNextValue(rar2);
                            waitHandle.Set();
                        },
                        null);
                    },
                    null);
                },
                null);
            },
            null);

            waitHandle.WaitOne(4000);
            Assert.Equal("\"", value);
        }

        [Fact]
        public void Should_read_third_row_third_cell_correctly()
        {
            string value = "Fail";
            var waitHandle = new ManualResetEvent(false);
            reader.BeginMoveToNextRow((nar) =>
            {
                reader.EndMoveToNextRow(nar);
                reader.BeginMoveToNextRow((nar2) =>
                {
                    reader.EndMoveToNextRow(nar2);
                    reader.BeginReadNextValue((rar) =>
                    {
                        reader.BeginReadNextValue((rar2) =>
                        {
                            reader.BeginReadNextValue((rar3) =>
                            {
                                value = reader.EndReadNextValue(rar3);
                                waitHandle.Set();
                            },
                            null);
                        },
                        null);
                    },
                    null);
                },
                null);
            },
            null);

            waitHandle.WaitOne(4000);
            Assert.Equal("\"quoted\"", value);
        }

        private readonly TiddlyCsvReader reader;
        private readonly Stream stream;
        private AutoResetEvent waitHandle = new AutoResetEvent(false);
    }
}
