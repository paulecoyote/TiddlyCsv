﻿
namespace Tiddly.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Xunit;    

    public class ReadAsRowsBadDataTests : IDisposable
    {
        public class TestRow
        {
            public string StringVal {get; set; }
            public bool BoolVal {get; set; }
            public int IntVal {get; set; }
            public float FloatVal {get; set; }        
        }

        public ReadAsRowsBadDataTests()
        {
            stream = File.Open("data/badrowstest.csv", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);            
        }

        public void Dispose()
        {

            stream.Close();
        }

        [Fact]
        public void Should_throw_expected_exception_type()
        {
            try
            {
                var reader = new TiddlyCsvReader(stream);
                var rows = reader.EndReadDocumentAsRows<TestRow>(
                    reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Assert.IsType(typeof(ArgumentOutOfRangeException), ex.InnerException);
            }
        }

        [Fact]
        public void Should_throw_exception_with_correct_column_name_in_message()
        {
            // Arrange
            ArgumentOutOfRangeException result = null;

            // Act
            try
            {
                var reader = new TiddlyCsvReader(stream);
                var rows = reader.EndReadDocumentAsRows<TestRow>(
                    reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                result = (ArgumentOutOfRangeException)ex.InnerException;
            }

            // Assert
            Assert.Contains("IntVal", result.Message);
        }

        [Fact]
        public void Should_throw_exception_with_correct_column_name_in_ParamName()
        {
            // Arrange
            ArgumentOutOfRangeException result = null;

            // Act
            try
            {
                var reader = new TiddlyCsvReader(stream);
                var rows = reader.EndReadDocumentAsRows<TestRow>(
                    reader.BeginReadDocumentAsRows<TestRow>(null, null, null), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                result = (ArgumentOutOfRangeException)ex.InnerException;
            }

            // Assert
            Assert.Equal("IntVal", result.ParamName);
        }

        private readonly Stream stream;
        private AutoResetEvent waitHandle = new AutoResetEvent(false);
    }
}
