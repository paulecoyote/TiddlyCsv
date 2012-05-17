using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Xunit;

namespace Tiddly.Tests
{
    public class ReadAsRowsTests : IDisposable
    {
        public class TestRow
        {
            public string StringVal {get; set; }
            public bool BoolVal {get; set; }
            public int IntVal {get; set; }
            public float FloatVal {get; set; }        
        }

        public ReadAsRowsTests()
        {
            stream = File.Open("data/rowstest.csv", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            reader = new TiddlyCsvReader(stream);
        }

        public void Dispose()
        {
            stream.Close();
        }

        [Fact]
        public void Should_read_document_1st_row_correctly()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal("a", rows[0].StringVal);
            Assert.Equal(true, rows[0].BoolVal);
            Assert.Equal(-2, rows[0].IntVal);
            Assert.Equal((float)-2.2, rows[0].FloatVal);
        }

        [Fact]
        public void Should_read_document_2nd_row_correctly()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal("b", rows[1].StringVal);
            Assert.Equal(false, rows[1].BoolVal);
            Assert.Equal(-1, rows[1].IntVal);
            Assert.Equal((float)-1, rows[1].FloatVal);
        }

        [Fact]
        public void Should_read_document_3rd_row_correctly()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal("c", rows[2].StringVal);
            Assert.Equal(true, rows[2].BoolVal);
            Assert.Equal(0, rows[2].IntVal);
            Assert.Equal((float)0, rows[2].FloatVal);
        }

        [Fact]
        public void Should_read_document_4th_row_correctly()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal("d", rows[3].StringVal);
            Assert.Equal(false, rows[3].BoolVal);
            Assert.Equal(1, rows[3].IntVal);
            Assert.Equal((float)1, rows[3].FloatVal);
        }

        [Fact]
        public void Should_read_document_5th_row_correctly()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal("é", rows[4].StringVal);
            Assert.Equal(true, rows[4].BoolVal);
            Assert.Equal(2, rows[4].IntVal);
            Assert.Equal((float)2.2, rows[4].FloatVal);
        }

        [Fact]
        public void Should_read_document_with_5_rows()
        {
            var rows = reader.EndReadDocumentAsRows<TestRow>(
                reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);

            Assert.Equal(5, rows.Count);
        }

        private readonly TiddlyCsvReader reader;
        private readonly Stream stream;
        private AutoResetEvent waitHandle = new AutoResetEvent(false);
    }
}
