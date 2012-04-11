
namespace Tiddly
{
    using System;    
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// Asyncronously read values from a stream.
    /// </summary>
    public class TiddlyCsvReader
    {
        /// <summary>
        /// Constructs a new CsvReader
        /// </summary>
        /// <param name="stream">Stream to read.  Does not close or dispose this stream.</param>
        public TiddlyCsvReader(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (!stream.CanRead)
                throw new ArgumentException("Stream not legible", "stream");

            this.decoder = streamEncoding.GetDecoder();
            this.stream = stream;
        }

        /// <summary>
        /// Skips values until the next row is reached.
        /// </summary>
        /// <param name="callback">Function to call when operation is complete.</param>
        /// <param name="state">State available to callback.</param>
        /// <returns>Async result to be used with EndMoveToNextRow.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Async operations need to catch all exceptions and put them in the async result")]
        public IAsyncResult BeginMoveToNextRow(AsyncCallback callback, object state)
        {
            var asyncResult = new TiddlyAsyncResult<Boolean>(callback, state);
            try
            {
                if (Thread.VolatileRead(ref isReadOperationPending) != 0)
                {
                    throw new InvalidOperationException("Only one read can be in flight at once, or order of bytes read from stream becomes indeterminate");
                }

                // Send back null if we are at the end of the row
                if (isEndOfFile)
                {
                    asyncResult.Success(false, true);
                }
                else if (isEndOfRow)
                {
                    // Allow our iterator to move on
                    isEndOfRow = false;
                    asyncResult.Success(true, true);
                }
                else
                {
                    // Skip until we are at the end of a row, then move past it
                    BeginMoveToNextRowSkipper(asyncResult);
                }
            }
            catch (Exception ex)
            {
                asyncResult.Fail(ex, true);
            }
            return asyncResult;
        }

        /// <summary>
        /// Read a csv value from the stream.
        /// </summary>
        /// <param name="callback">Function to call when operation is complete.</param>
        /// <param name="state">State available to callback.</param>
        /// <returns>Async result to be used with EndReadNextValue.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Async operations need to catch all exceptions and put them in the async result")]
        public IAsyncResult BeginReadNextValue(AsyncCallback callback, object state)
        {
            var asyncResult = new TiddlyAsyncResult<String>(callback, state);
            try
            {
                if (Thread.VolatileRead(ref isReadOperationPending) != 0)
                {
                    throw new InvalidOperationException("Only one read can be in flight at once, or order of bytes read from stream becomes indeterminate");
                }

                BeginReadNextColumnStringValueFromFileStream(asyncResult);
            }
            catch (Exception ex)
            {
                asyncResult.Fail(ex, true);
            }

            return asyncResult;
        }

        /// <summary>
        /// Skips values until the next row is reached.
        /// </summary>
        /// <param name="callback">Function to call when operation is complete.</param>
        /// <param name="state">State available to callback.</param>
        /// <returns>Async result to be used with EndMoveToNextRow.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Async operations need to catch all exceptions and put them in the async result")]
        public IAsyncResult BeginReadDocumentAsColumns(Func<Int32, Int32, Int32, Boolean> progressCallback, AsyncCallback callback, object state)
        {
            var documentAsyncResult = new TiddlyAsyncResult<IList<IList<String>>>(callback, state);
            var document = new List<IList<String>>();

            try
            {                
                if (Thread.VolatileRead(ref isReadOperationPending) != 0)
                {
                    throw new InvalidOperationException("Only one read can be in flight at once, or order of bytes read from stream becomes indeterminate");
                }

                ReadDocumentColumns(progressCallback, documentAsyncResult, document, 0, 0);
            }
            catch (Exception ex)
            {
                documentAsyncResult.Fail(ex, true);
            }
            
            return documentAsyncResult;
        }

        private void ReadDocumentColumns(Func<Int32, Int32, Int32, Boolean> progressCallback, TiddlyAsyncResult<IList<IList<String>>> documentAsyncResult,
            List<IList<String>> document, int column, int row)
        {
            try
            {
                BeginReadNextValue(ar =>
                    {
                        try
                        {
                            var cell = EndReadNextValue(ar);
                            if (cell == null)
                            {
                                if (isEndOfFile)
                                {
                                    // Finished reading the document
                                    documentAsyncResult.Success(document, ar.CompletedSynchronously);
                                }
                                else
                                {
                                    // Add nulls to columns if row has terminated without populating them.
                                    for (var i=column; i<document.Count; ++i) { document[i].Add(null); }

                                    // Move to next row and continue reading
                                    EndMoveToNextRow(BeginMoveToNextRow(null, null));
                                    ReadDocumentColumns(progressCallback, documentAsyncResult, document, 0, ++row);
                                }
                            }
                            else
                            {
                                if (column >= document.Count)
                                {
                                    // If we don't have a column, create it
                                    var newColumn = new List<String>();

                                    // If this column as zinged in to existence later on, populate all above with null
                                    for (var i=0; i<row-1; ++ i) { newColumn.Add(null); }
                                    document.Add(newColumn);
                                }

                                document[column].Add(cell);
                                if (progressCallback == null || progressCallback(bytesRead, column, row))
                                {
                                    ReadDocumentColumns(progressCallback, documentAsyncResult, document, ++column, row);
                                }
                                else
                                {
                                    // Document reading cancelled at progress callback request
                                    documentAsyncResult.Success(document, ar.CompletedSynchronously);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            documentAsyncResult.Fail(ex, true);
                        } 
                    },
                    null);
            }
            catch (Exception ex)
            {
                documentAsyncResult.Fail(ex, true);
            }
        }
        
        /// <summary>
        /// Result of move to next row. Must be called in a pair with BeginMoveToNextRow.
        /// </summary>
        /// <param name="asyncResult">Async result from BeginMoveToNextRow.</param>
        /// <param name="timeout">Timeout for operation before exception is thrown.</param>
        /// <returns>True if at start of new row.</returns>
        /// <remarks>Will block if operation is not yet complete until the timeout is reached.</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed", Justification = "Only worried about C# clients for the moment")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "This method is part of the api for the class")]
        public Boolean EndMoveToNextRow(IAsyncResult asyncResult, Int32 timeout = 25000) // Timeout.Infinite
        {
            var operation = (TiddlyAsyncResult<Boolean>)asyncResult;
            return operation.End(timeout);
        }

        /// <summary>
        /// Next csv value.  Must be called in a pair with BeginReadNextValue.
        /// </summary>
        /// <param name="asyncResult">Async result from BeginMoveToNextRow.</param>
        /// <param name="timeout">Timeout for operation before exception is thrown.</param>
        /// <returns>True if at start of new row.</returns>
        /// <remarks>Will block if operation is not yet complete until the timeout is reached.</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed", Justification = "Only worried about C# clients for the moment")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "This method is part of the api for the class")]
        public String EndReadNextValue(IAsyncResult asyncResult, Int32 timeout = 25000) // Timeout.Infinite
        {
            var operation = (TiddlyAsyncResult<String>)asyncResult;
            return operation.End(timeout);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1026:DefaultParametersShouldNotBeUsed", Justification = "Only worried about C# clients for the moment")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "This method is part of the api for the class")]
        public IList<IList<String>> EndReadDocumentAsColumns(IAsyncResult asyncResult, Int32 timeout = 25000) // Timeout.Infinite
        {
            var operation = (TiddlyAsyncResult<IList<IList<String>>>)asyncResult;
            return operation.End(timeout);
        }

        /// <summary>
        /// Helper function for BeginMoveToNextRow.  Reads and discards values until next row is found.
        /// </summary>
        /// <param name="finalResult">Final result used to signal completion to user.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Async operations need to catch all exceptions and put them in the async result")]
        private void BeginMoveToNextRowSkipper(TiddlyAsyncResult<Boolean> finalResult)
        {
            // Async recursive function
            BeginReadNextValue((skipResult) =>
                {
                    try
                    {
                        // Read, don't even care about result, just reader state.
                        EndReadNextValue(skipResult);

                        if (isEndOfFile)
                        {
                            // Successful, but nothing else to read
                            finalResult.Success(false, skipResult.CompletedSynchronously);
                        }
                        else if (isEndOfRow)
                        {
                            // Allow our iterator to move on
                            isEndOfRow = false;
                            finalResult.Success(false, skipResult.CompletedSynchronously);
                        }
                        else
                        {
                            // Repeat (rather than recurisve) call in to this function
                            BeginMoveToNextRowSkipper(finalResult);
                        }
                    }
                    catch (Exception ex)
                    {
                        finalResult.Fail(ex, skipResult.CompletedSynchronously);
                    }
                },
                finalResult);
        }

        /// <summary>
        /// Calls stream BeginRead if appropriate to fill the input buffer.
        /// </summary>
        /// <param name="finalResult">Async result used to indicate success to the user of the api.</param>
        private void BeginReadNextColumnStringValueFromFileStream(TiddlyAsyncResult<String> finalResult)
        {
            // Send back null if we are at the end of the row
            if (isEndOfRow || isEndOfFile)
            {
                finalResult.Success(null, true);
            }
            else
            {
                if (columnValueIterator == null)
                {
                    Interlocked.CompareExchange(ref columnValueIterator, ProcessStringValueFromBuffer(finalResult).GetEnumerator(), null);
                    if (Interlocked.CompareExchange(ref isReadOperationPending, 1, 0) != 0)
                    {
                        finalResult.Fail(
                            new InvalidOperationException("Stream must not be read from by different thread simultaneously.  Order becomes unknown."),
                            finalResult.CompletedSynchronously);
                        return;
                    }

                    // First read into the buffer
                    stream.BeginRead(
                        byteBuffer,
                        0,
                        byteBuffer.Length,
                        (far) => ReadIntoBufferAction(far),
                        finalResult);
                }
                else
                {
                    if (columnValueIterator.MoveNext())
                    {
                        ReadValueOrRepeat(finalResult, finalResult.CompletedSynchronously);
                    }
                    else
                    {
                        finalResult.Fail(
                            new InvalidOperationException("Could not parse csv stream"),
                            finalResult.CompletedSynchronously);
                    }
                }
            }
        }

        /// <summary>
        /// Enumerates through a csv file
        /// </summary>
        /// <param name="finalResult">Final async result used by user</param>
        /// <returns>Column value, or null to indicate end of row.</returns>
        private IEnumerable<String> ProcessStringValueFromBuffer(TiddlyAsyncResult<String> finalResult)
        {
            Int32 quotes = 0;
            Char[] charBuffer = new Char[byteBuffer.Length];
            Int32 charsToReadCount = -1, charsDecodedCount = -1;

            Boolean quotedCell = false, seenFirstHalfOfQuotedQuote = false;
            Char previousChar = default(Char);
            var sb = new StringBuilder();
            Int32 currentPosition = 0;
            decodedBufferedSection = 0;

            // This loop is reentrant and relies on a read elsewhere to populate the buffer and bytesRead.
            while (bytesRead != 0)
            {
                charsToReadCount = decoder.GetCharCount(
                     byteBuffer,
                     0,
                     bytesRead);

                // Must resize char buffer to accomodate everything read
                if (charsToReadCount > charBuffer.Length)
                {
                    Array.Resize<Char>(ref charBuffer, charsToReadCount);
                }

                charsDecodedCount = decoder.GetChars(
                    byteBuffer,
                    0,
                    bytesRead,
                    charBuffer,
                    0);
                currentPosition = 0;
                decodedBufferedSection = bufferedSection;

                while (currentPosition < charsDecodedCount)
                {
                    var c = charBuffer[currentPosition];

                    if (c == '\"')
                    {
                        ++quotes;
                        if (quotedCell)
                        {
                            if (seenFirstHalfOfQuotedQuote)
                            {
                                sb.Append(c);
                                seenFirstHalfOfQuotedQuote = false;
                            }
                            else
                            {
                                seenFirstHalfOfQuotedQuote = true;
                            }
                        }
                        else
                        {
                            quotedCell = true;
                        }
                    }
                    else
                    {
                        // Any other character clears off quoted quote flag.
                        seenFirstHalfOfQuotedQuote = false;

                        if (c == ',')
                        {
                            if (!quotedCell || ((quotes & 1) == 0))
                            {
                                // Either unquoted cell, or the speech marks are balanced (even)
                                yield return sb.ToString();
                                sb.Length = quotes = 0;
                                quotedCell = false;
                            }
                            else
                            {
                                // Embedded comma
                                sb.Append(c);
                            }
                        }
                        else if (c == '\n')
                        {
                            // End of row
                            isEndOfRow = true;
                            if (previousChar == '\r')
                            {
                                // Ignore DOS.  
                                // A Mac version would break on \r... an exercise for the reader.
                                yield return sb.ToString(0, sb.Length - 1);
                            }
                            else
                            {
                                yield return sb.ToString();
                            }

                            // Return null to signal we still have something to do, but not in correct state.
                            while (isEndOfRow) yield return null;

                            // Move value start to next row, set length to zero.
                            sb.Length = 0;
                        }
                        else
                        {
                            // Non control character, just add to our buffer
                            sb.Append(c);
                        }
                    }

                    previousChar = c;
                    ++currentPosition;
                }

                // Check to see if buffer has been updated
                if (bytesRead != 0 && bufferedSection == decodedBufferedSection)
                {
                    // Need to read more before continuing
                    yield return null;
                }
            }

            // while bytesRead != 0 is no longer true.
            // Stream uses 0 bytes read to indicate end of file.
            isEndOfRow = isEndOfFile = true;
            if (sb.Length > 0)
            {
                // This means there's no line break on the final line.
                // Return what ever is left then clear the buffer.
                yield return sb.ToString();
                sb.Length = 0;
            }

            // Further calls to this iterator's next will now return false.
        }

        /// <summary>
        /// Action taken from BeginRead on Stream
        /// </summary>
        /// <param name="far">Async result from stream.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Async operations need to catch all exceptions and put them in the async result")]
        private void ReadIntoBufferAction(IAsyncResult far)
        {
            // File async read complete
            TiddlyAsyncResult<String> finalResult = null;

            try
            {
                // Our async result was set as the state of Stream's async result.
                finalResult = (TiddlyAsyncResult<String>)far.AsyncState;
                bytesRead = stream.EndRead(far);
                ++bufferedSection;

                if (columnValueIterator.MoveNext())
                {
                    // Read buffer used, signal read operation complete
                    Interlocked.Exchange(ref isReadOperationPending, 0);
                    ReadValueOrRepeat(finalResult, far.CompletedSynchronously);
                }
                else
                {
                    if (bytesRead == 0)
                    {
                        // End of file.  Nothing left, success.
                        // (Probably due to end of line on last line of data)
                        finalResult.Success(null, far.CompletedSynchronously);
                    }
                    else
                    {
                        throw new InvalidOperationException("Failed to parse csv from stream");
                    }                    
                }
            }
            catch (Exception ex)
            {
                // Read complete, but with errors.
                Interlocked.Exchange(ref isReadOperationPending, 0);
                finalResult.Fail(ex, far.CompletedSynchronously);
            }
        }

        /// <summary>
        /// Either completes our async read value operation, or reads again.
        /// </summary>
        /// <param name="finalResult">Result user our api is listening for.</param>
        /// <param name="completedSynchronously">Indicates if operation so far has completed synchronously or not.</param>
        private void ReadValueOrRepeat(TiddlyAsyncResult<String> finalResult, bool completedSynchronously)
        {
            if (!isEndOfRow && null == columnValueIterator.Current)
            {
                if (Interlocked.CompareExchange(ref isReadOperationPending, 1, 0) != 0)
                {
                    finalResult.Fail(
                        new InvalidOperationException("Stream must not be read from by different thread simultaneously.  Order becomes unknown."),
                        finalResult.CompletedSynchronously);
                    return;
                }

                // Need to feed the iterator more input
                stream.BeginRead(
                        byteBuffer,
                        0,
                        byteBuffer.Length,
                        (far) => ReadIntoBufferAction(far),
                        finalResult);
            }
            else
            {
                // End of the row, or just nothing to read.
                finalResult.Success(
                    columnValueIterator.Current,
                    completedSynchronously);
            }
        }

        private const Int32 bufferSize = 1 << 16;
        private readonly Encoding streamEncoding = Encoding.UTF8;
        private Byte[] byteBuffer = new Byte[bufferSize];
        private UInt32 bufferedSection;
        private Int32 bytesRead;
        private IEnumerator<String> columnValueIterator;
        private UInt32 decodedBufferedSection;
        private Decoder decoder;
        private Boolean isEndOfFile;
        private Boolean isEndOfRow;
        private Int32 isReadOperationPending;
        private Stream stream;
    }

    /// <summary>
    /// Async result used by tiddly.
    /// </summary>
    /// <typeparam name="TState">Type of state</typeparam>
    /// <typeparam name="TResult">Type of result stored</typeparam>
    internal class TiddlyAsyncResult<TResult> : IAsyncResult
    {
        /// <summary>
        /// Constructs a new TiddlyAsyncResult.
        /// </summary>
        /// <param name="asyncState">Async result passed in to the BeginFoo method</param>
        public TiddlyAsyncResult(AsyncCallback callback, object asyncState)
        {
            this.asyncState = asyncState;
            this.callback = callback;
        }

        /// <summary>
        /// Async result from Begin... method.
        /// </summary>
        public object AsyncState
        {
            get { return asyncState; }
        }

        /// <summary>
        /// Gets a System.Threading.WaitHandle that is used to wait for an asynchronous
        /// operation to complete.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "eventSync is disposed if it is not set to asyncWaitHandler class member")]
        public WaitHandle AsyncWaitHandle
        {
            get
            {
                if (null == asyncWaitHandler)
                {
                    var eventSync = new ManualResetEvent(false);
                    if (null != Interlocked.CompareExchange<ManualResetEvent>(
                        ref asyncWaitHandler,
                        eventSync,
                        null))
                    {
                        // Kill the spare!
                        // Only happens if more than one thread tries to 
                        // access AsyncWaitHandle for the first time. 
                        eventSync.Dispose();
                    }
                }

                return asyncWaitHandler;
            }
        }

        /// <summary>
        /// True means the wait handle does not 
        /// need to be used (already completed).
        /// </summary>
        /// <remarks>
        /// Set to true if completed 
        /// </remarks>
        public bool CompletedSynchronously
        {
            get
            {
                return Thread.VolatileRead(ref completedState) ==
                    CompletedSynchronouslyState;
            }
        }

        /// <summary>
        /// Flags that the operation is pending or complete.
        /// </summary>
        public bool IsCompleted
        {
            get
            {
                return Thread.VolatileRead(ref this.completedState) != CompletePending;
            }
        }

        /// <summary>
        /// Result of operation.
        /// </summary>
        internal TResult Result
        {
            get { return result; }
        }

        /// <summary>
        /// Called by operation to indicate failure
        /// </summary>
        /// <param name="failure"></param>
        /// <param name="completedSynchronously"></param>
        internal void Fail(Exception failure, bool completedSynchronously)
        {
            // completedSynchronously field MUST be set prior calling the callback
            Thread.VolatileWrite(
                ref this.completedState,
                completedSynchronously ? CompletedSynchronouslyState : CompletedAsynchronouslyState);
            this.exception = failure;

            // If event is being used, flag complete
            if (null != asyncWaitHandler) asyncWaitHandler.Set();

            // If we have a call back, call it
            if (null != callback) callback(this);
        }

        /// <summary>
        /// Called by operation to indicate success
        /// </summary>
        /// <param name="successfulResult"></param>
        /// <param name="completedSynchronously"></param>
        internal void Success(TResult successfulResult, bool completedSynchronously)
        {
            Thread.VolatileWrite(
                ref this.completedState,
                completedSynchronously ? CompletedSynchronouslyState : CompletedAsynchronouslyState);
            this.result = successfulResult;

            // If event is being used, flag complete
            if (null != asyncWaitHandler) asyncWaitHandler.Set();

            // If we have a call back, call it
            if (null != callback) callback(this);
        }

        /// <summary>
        /// Call as part of the End of an async result.  
        /// Returns result or throws exception.  
        /// Blocks if called using EndFoo(BeginFoo) style used instead of callback.
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout in ms or System.Threading.Timeout.Infinite.</param>
        /// <returns>Result stored by async operation.</returns>
        /// <remarks>Expects single call from single thread.</remarks>
        internal TResult End(int millisecondsTimeout)
        {
            if (Thread.VolatileRead(ref this.completedState) == CompletePending)
            {
                // Grab wait handle, wait on it, dispose and nullify when done.
                var waitHandle = (ManualResetEvent)AsyncWaitHandle;
                if (waitHandle.WaitOne(millisecondsTimeout))
                {
                    // Current wait handle no longer required, it has been signaled.
                    Interlocked.CompareExchange<ManualResetEvent>(ref this.asyncWaitHandler, null, waitHandle);
                    waitHandle.Dispose();
                }
                else
                {
                    // Let the wait handle live on until collected.
                    throw new TimeoutException();
                }
            }

            if (null != exception) throw exception;

            return result;
        }

        private const Int32 CompletedAsynchronouslyState = 1;
        private const Int32 CompletedSynchronouslyState = 2;
        private const Int32 CompletePending = 0;
        private object asyncState;
        private ManualResetEvent asyncWaitHandler;
        private AsyncCallback callback;
        private Int32 completedState;
        private Exception exception;
        private TResult result;
    }
}
